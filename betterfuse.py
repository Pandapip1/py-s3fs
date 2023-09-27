try:
    import _find_fuse_parts
except ImportError:
    pass
import fuse, ast, inspect
from abc import abstractmethod

fuse.fuse_python_api = (0, 2)
if not hasattr(fuse, '__version__'):
    raise RuntimeError('your fuse-py doesn\'t know of fuse.__version__, probably it\'s too old.')

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

class FuseOption:
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

class FuseMeta(type):
    def __call__(cls, *args, **kwargs):
        # Create the instance using the superclass's __call__ method
        instance = super(FuseMeta, cls).__call__(*args, **kwargs)
        
        # If we don't have a mountpoint, don't preAttach
        if instance.fuse_args.mountpoint is not None:
            instance.preAttach()

        # If we don't have a mountpoint and aren't helping or versioning, enable help and show usage
        if instance.fuse_args.mountpoint is None and not instance.fuse_args.modifiers['showhelp'] and not instance.fuse_args.modifiers['showversion']:
            instance.fuse_args.modifiers['showhelp'] = True
            print(instance.parser.format_help())
        
        return instance

class FuseApplication(fuse.Fuse, metaclass=FuseMeta):
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
        
        # Parse the command line
        self.parse(errex=1)
    
    @abstractmethod
    def preAttach(self):
        '''
        Called before the filesystem is attached.
        '''
        pass
