try:
    import _find_fuse_parts
except ImportError:
    pass
import fuse, ast, inspect
from abc import abstractmethod

fuse.fuse_python_api = (0, 2)
if not hasattr(fuse, '__version__'):
    raise RuntimeError('python-fuse doesn\'t have a __version__ attribute. It might be outdated.')

def extract_docstring_after_class_attribute(klass, attribute_name):
    '''Extract the docstring associated with a class attribute using AST.'''
    source = inspect.getsource(klass)
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            for idx, class_node in enumerate(node.body):
                if isinstance(class_node, ast.AnnAssign):
                    if class_node.target.id == attribute_name:
                        # Check if the next node is a string (docstring)
                        next_node = node.body[idx + 1] if idx + 1 < len(node.body) else None
                        if next_node and isinstance(next_node, ast.Expr) and (
                            isinstance(next_node.value, (ast.Str, ast.Constant)) and isinstance(next_node.value.value, str)
                        ):
                            return next_node.value.value.strip()
    return None

class FuseOption(object):
    def __init__(self, default=None):
        self.default = default
        self.name = None
        self.opt_type = None
        self.docstring = None

    def __set_name__(self, owner, name):
        self.name = name

        # Get the type from the type annotation
        type_annotation = owner.__annotations__.get(name, None)
        if type_annotation == int:
            self.opt_type = 'int'
        elif type_annotation == float:
            self.opt_type = 'float'
        elif type_annotation == bool:
            self.opt_type = 'bool'
        else:
            self.opt_type = 'string'

        # Get the docstring using AST
        self.docstring = extract_docstring_after_class_attribute(owner, name)
            
    def __get__(self, instance, owner):
        return getattr(instance.cmdline[0], self.name, self.default)

    def __set__(self, instance, value):
        setattr(instance.cmdline[0], self.name, value)

class FuseApplication(fuse.Fuse):
    '''
    A subclass of fuse.Fuse that uses the class's attributes to add options to the command line, and automatically parses it.
    '''

    def __init__(self, **kwargs):
        # Set required attributes
        super().__init__(
            **(kwargs | {
                'version': '%prog ' + fuse.__version__,
                'usage': inspect.getdoc(type(self)) + '\n\n' + fuse.Fuse.fusage,
                'dash_s_do': 'setsingle'
            })
        )

        # Generate options
        for name, attr in type(self).__dict__.items():
            if isinstance(attr, FuseOption):
                if attr.opt_type == 'bool':
                    self.parser.add_option(f'--{name}', action='store_true', attr=attr.default, help=attr.docstring)
                else:
                    self.parser.add_option(f'--{name}', action='store', type=attr.opt_type, default=attr.default, help=attr.docstring)
    
    def main(self, *args, **kwargs):
        self.parse(errex=1)

        if self.fuse_args.mountpoint is None and not self.fuse_args.modifiers['showhelp'] and not self.fuse_args.modifiers['showversion']:
            self.fuse_args.modifiers['showhelp'] = True
            print(self.parser.format_help())

        self.premain()

        # Call the superclass's main method
        super().main(*args, **kwargs)
    
    @abstractmethod
    def premain(self):
        '''
        Called before the filesystem is attached.
        '''
        pass
