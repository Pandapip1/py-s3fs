from betterfuse import FuseApplication, FuseOption
import os, stat, errno, inspect, datetime, boto3, botocore, configparser, fuse, io, types, struct
from mypy_boto3_s3.service_resource import S3ServiceResource, Bucket, Object, ObjectVersion
from threading import Lock

def _normpath(path):
    '''
    Normalize a path even more than os.path.normpath.
    '''
    # Ensure the path is absolute relative to the current directory as root
    if not path.startswith('/'):
        abs_path = '/' + path
    else:
        abs_path = path
    
    # Ensure it does not end in a slash
    if abs_path.endswith('/') and abs_path != '/':
        abs_path = abs_path[:-1]
    
    # Resolve . and ..
    resolved_path = os.path.normpath(abs_path)
    
    # Return the processed path
    return resolved_path

def _relnormpath(path):
    '''
    Returns a relative normalized path, relative to the root directory.

    (Basically _normpath but without the leading slash)
    '''
    normedpath = _normpath(path)
    if normedpath == '/':
        return '.'
    return normedpath[1:]

class S3Fs(FuseApplication):
    '''
    A filesystem that connects to an S3 account and exposes its buckets, objects, and configuration.
    '''

    config_file: str = FuseOption(default='~/.aws/config')
    '''
    The path to the AWS config file.
    '''

    cache_dir: str = FuseOption(default='/tmp/s3fs')
    '''
    The path to the cache directory.
    '''

    s3: S3ServiceResource = None
    '''
    The boto3 S3 resource.
    '''

    filesystem: dict[str, "S3Path"] = None
    '''
    A dictionary of paths to S3Path objects.
    '''

    class S3Path(object):
        '''
        A helper class for representing S3 directories and files.
        '''

        cache_dir: str = None
        '''
        The path to the cache directory.
        '''

        s3: S3ServiceResource = None
        '''
        The boto3 S3 resource.
        '''

        bucket: Bucket = None
        '''
        The bucket that this path is in.
        '''

        path: str = None
        '''
        The path of the file or directory.
        '''

        isdir: bool = None
        '''
        Whether or not this path is a directory.
        '''

        obj: Object = None
        '''
        The object that this path represents, if it is a file.
        '''

        version: ObjectVersion = None
        '''
        The version of the object that this path represents, if it is a file.
        '''

        _lock: Lock = None
        '''
        A lock for this path.
        '''

        listing: set[str] = None
        '''
        A set of files and directories in this directory, if it is a directory.
        '''

        dirty: bool = None
        '''
        Whether or not this file is dirty (needs to be uploaded on flush), if it is a file.
        '''

        offsets: set[tuple[int]] = None
        '''
        A set of tuples of downloaded offsets, if it is a file.
        '''

        file: io.BufferedRandom = None
        '''
        The file object, if it is a file.
        '''

        fd: int = None
        '''
        The file descriptor, if it is a file.
        '''
        
        @classmethod
        def _add_offsets(cls, offsetspos1: set[tuple[int]], offsetspos2: set[tuple[int]]):
            # Convert sets to lists, combine them, and sort based on the starting value of each tuple
            combined = sorted(list(offsetspos1) + list(offsetspos2), key=lambda x: x[0])

            result = []
            stack = []

            for current_range in combined:
                if not stack:
                    stack.append(current_range)
                else:
                    top = stack[-1]

                    # If the current range overlaps or is contiguous with the top of the stack
                    if current_range[0] <= top[1] + 1:
                        # Merge the two ranges
                        merged_range = (top[0], max(top[1], current_range[1]))
                        stack.pop()
                        stack.append(merged_range)
                    else:
                        # If they don't overlap or aren't contiguous, push the current range onto the stack
                        stack.append(current_range)

            # Add the merged ranges to the result
            result.extend(stack)

            return set(result)

        @classmethod
        def _subtract_offsets(cls, offsetspos: set[tuple[int]], offsetsneg: set[tuple[int]]):
            # Convert sets to lists and sort them based on the starting value of each tuple
            offsetspos = sorted(list(offsetspos), key=lambda x: x[0])
            offsetsneg = sorted(list(offsetsneg), key=lambda x: x[0])

            result = []
            j = 0
            for pos_range in offsetspos:
                while j < len(offsetsneg) and offsetsneg[j][1] <= pos_range[0]:
                    j += 1
                temp_range = pos_range
                while j < len(offsetsneg) and offsetsneg[j][0] < temp_range[1]:
                    if offsetsneg[j][0] > temp_range[0]:
                        result.append((temp_range[0], offsetsneg[j][0]))
                    temp_range = (max(temp_range[0], offsetsneg[j][1]), temp_range[1])
                    j += 1
                if temp_range[0] < temp_range[1]:
                    result.append(temp_range)

            return set(result)

        def __init__(self, s3, path, cache_dir, create=None):
            '''
            Initialize the path.
            '''
            # Ensure path is absolute and normalized
            path = _normpath(path)

            if path.lower().endswith('.s3fs'):
                raise Exception('Cannot create files or directories ending in .s3fs; this is reserved for internal use')

            # Set basic information
            self.s3 = s3
            self.path = path
            self.cache_dir = cache_dir

            if path == '/':
                # Special case for root directory
                if create is not None:
                    raise Exception('Cannot create root directory')
                self.isdir = True
                self.listing = set()
                self.bucket = None
                # Now populate the listing
                for bucket in self.s3.buckets.all():
                    self.listing.add(bucket.name)
            else:
                # Get basic information about the path
                path_parts = path.split('/')[1:]

                # If create is not None, create the S3 data structures
                if create is not None:
                    self.isdir = create['isdir']
                    self.listing = set()
                    if self.isdir:
                        if self.isbucket:
                            self.bucket = self.s3.create_bucket(Bucket=path_parts[0])
                            self.bucket.Versioning().enable()
                            self.bucket.LifecycleConfiguration().put(
                                LifecycleConfiguration={
                                    'Rules': [
                                        {
                                            'ID': 'expire-old-versions',
                                            'Prefix': '',
                                            'Status': 'Enabled',
                                            'NoncurrentVersionExpiration': {
                                                'NoncurrentDays': 3
                                            }
                                        }
                                    ]
                                }
                            )
                        else:
                            self.bucket = self.s3.Bucket(path_parts[0])
                    else:
                        # Upload an empty file to S3
                        self.bucket = self.s3.Bucket(path_parts[0])
                        self.bucket.upload_fileobj(
                            Fileobj=io.BytesIO(b''),
                            Key='/'.join(path_parts[1:]),
                            ExtraArgs=dict(
                                Metadata=dict(
                                    s3fs=dict(
                                        isdir=False,
                                        dirty=False
                                    )
                                )
                            )
                        )
                else: # Populate the stuff that would have been populated by create (bucket, isdir, listing)
                    self.bucket = self.s3.Bucket(path_parts[0])
                    effectivepath = '/'
                    if not self.isbucket:
                        effectivepath = _normpath('/'.join(path_parts[1:]))
                    # We're a directory if there's a subfile or subdirectory or if we're a bucket
                    self.isdir = self.isbucket or any([
                        _normpath(obj.key).startswith(effectivepath) and
                        _normpath(obj.key) != effectivepath
                        for obj in self.bucket.objects.all()
                    ])
                    # If we're a directory, populate the listing
                    if self.isdir:
                        self.listing = set([
                            _normpath(obj.key).split('/')[len(path_parts)]
                            for obj in self.bucket.objects.all()
                            if _normpath(obj.key).startswith(effectivepath) and
                            _normpath(obj.key) != effectivepath
                        ])
                
                # If we're a file, populate the file-specific stuff
                if not self.isdir:
                    self.obj = self.bucket.Object('/'.join(path_parts[1:]))
                    self.dirty = False
                    self._lock = Lock()
                    versions = list(self.bucket.object_versions.filter(Prefix=self.obj.key).all())
                    versions.sort(key=lambda x: x.last_modified)
                    self.version = versions[-1]
                    self.offsets = set()
                
                
                # Ensure the parent directory exists
                os.makedirs(os.path.dirname(os.path.join(self.cache_dir, _relnormpath(path))), exist_ok=True)
                # If we're a file, open the file
                if not self.isdir:
                    self.fd = os.open(os.path.join(self.cache_dir, _relnormpath(path)), os.O_RDWR | os.O_CREAT)
                    self.file = os.fdopen(self.fd, 'br+')
                    self.file.seek(0, os.SEEK_END)
                    self.file.truncate(self.obj.content_length)
                    self.file.flush()
                    self.file.seek(0)
                    # Chmod the file to 664 (rw-rw-r--) by default
                    os.chmod(os.path.join(self.cache_dir, _relnormpath(path)), 0o664)
                else:
                    # Ensure the directory exists
                    os.makedirs(os.path.join(self.cache_dir, _relnormpath(path)), exist_ok=True)
                    # Chmod the directory to 775 (rwxrwxr-x) by default
                    os.chmod(os.path.join(self.cache_dir, _relnormpath(path)), 0o775)
                # Now chmod all parent directories to 775 (rwxrwxr-x) by default
                for parent in range(len(path_parts) - 1):
                    os.chmod(os.path.join(self.cache_dir, _relnormpath(os.path.join(*path_parts[:parent + 1]))), 0o775)
        
        @property
        def isbucket(self):
            '''
            Whether or not this path is a bucket.
            '''
            return len(self.path.split('/')) == 2 and self.isdir
        
        @property
        def isrootdir(self):
            '''
            Whether or not this path is the root directory.
            '''
            return len(self.path.split('/')) == 1 and self.isdir
        
        def getattr(self):
            '''
            Get the stat of the path.
            '''
            st = fuse.Stat()
            st_cache = os.stat(os.path.join(self.cache_dir, _relnormpath(self.path)))
            if self.isdir:
                st.st_mode = stat.S_IFDIR | (st_cache.st_mode & 0o7777)
                st.st_nlink = 2 + len(self.listing)
            else:
                st.st_mode = stat.S_IFREG | (st_cache.st_mode & 0o7777)
                st.st_nlink = 1
                st.st_size = self.obj.content_length
            return st
        
        def getxattr(self, name, size):
            # Fetch the xattr from the cache file
            retval = os.getxattr(os.path.join(self.cache_dir, _relnormpath(self.path)), name)
            # If the size is 0, return the size of the xattr
            if size == 0:
                return len(retval)
            # Otherwise, return the xattr
            return retval
        
        def listxattr(self, size):
            # Fetch the xattrs from the cache file
            retval = os.listxattr(os.path.join(self.cache_dir, _relnormpath(self.path)))
            # If the size is 0, return the size of the xattrs (joint size, including null terminators)
            if size == 0:
                return sum([len(xattr) + 1 for xattr in retval])
            # Otherwise, return the xattrs
            return retval
        
        def setxattr(self, name, value, flags):
            # Set the xattr on the cache file
            os.setxattr(os.path.join(self.cache_dir, _relnormpath(self.path)), name, value, flags)
        
        def removexattr(self, name):
            # Remove the xattr from the cache file
            os.removexattr(os.path.join(self.cache_dir, _relnormpath(self.path)), name)

        def fgetattr(self):
            '''
            Get the stat of the file.
            '''
            if self.isdir:
                return -errno.EISDIR
            return self.getattr()

        def readdir(self, offset):
            '''
            Get the contents of the directory.
            '''
            if self.isdir:
                yield fuse.Direntry('.')
                yield fuse.Direntry('..')
                for entry in self.listing:
                    yield fuse.Direntry(entry)
            else:
                yield fuse.Direntry(os.path.basename(self.path))

        def open(self, flags):
            '''
            Open the file.
            '''
            if self.isdir:
                return -errno.EISDIR
        
        def lock(self, cmd, owner, **kargs):
            '''
            Lock the file. Not cross-platform compatible (Linux only, due to F_GETLK)
            '''
            fnctl = fuse.fnctl # Alias
            op = {
                fcntl.F_UNLCK : fcntl.LOCK_UN,
                fcntl.F_RDLCK : fcntl.LOCK_SH,
                fcntl.F_WRLCK : fcntl.LOCK_EX
            }[kargs['l_type']]
            if cmd == fuse.fnctl.F_GETLK:
                lockdata = struct.pack('hhQQi', kargs['l_type'], os.SEEK_SET, kargs['l_start'], kargs['l_len'], kargs['l_pid'])
                ld2 = fcntl.fcntl(self.fd, fcntl.F_GETLK, lockdata)
                flockfields = ('l_type', 'l_whence', 'l_start', 'l_len', 'l_pid')
                uld2 = struct.unpack('hhQQi', ld2)
                res = dict(zip(flockfields, uld2))
                return fuse.Flock(**res)
            elif cmd == fuse.fnctl.F_SETLK:
                if op != fcntl.LOCK_UN:
                    op |= fcntl.LOCK_NB
                fcntl.flock(self.fd, op)
            elif cmd == fnctl.F_SETLKW:
                pass # TODO: Implement blocking locks
            return -fuse.EINVAL

        def read(self, size, offset):
            '''
            Read from the file.
            '''
            if self.isdir:
                return -errno.EISDIR
            if offset >= self.obj.content_length:
                return b''
            if offset + size > self.obj.content_length:
                size = self.obj.content_length - offset
            # Get sections of file that haven't been downloaded yet
            sections = self._subtract_offsets({(offset, offset + size)}, self.offsets)
            # Download sections of file that haven't been downloaded yet, and acquire the lock
            with self._lock:
                for section in sections:
                    self.file.seek(section[0])
                    self.file.write(self.obj.get(Range=f'''bytes={section[0]}-{section[1]}''', VersionId=self.version.id).get('Body').read())
                self.file.flush()
                self.file.seek(offset)
                retval = self.file.read(size)
            # Add downloaded sections to the list and remove contiguous sections from the list
            self.offsets = self._add_offsets(self.offsets, {(offset, offset + size)})
            # Return the data
            return retval
        
        def write(self, buf, offset):
            '''
            Write to the file.
            '''
            if self.isdir:
                return -errno.EISDIR
            # Note that we need to read the entire file to write to it, because S3 doesn't support partial writes
            self.read(self.obj.content_length, 0)
            # Write to the file
            with self._lock:
                self.file.seek(offset)
                self.file.write(buf)
                self.file.flush()
            # Mark the file as dirty
            self.dirty = True
            # Return the number of bytes written
            return len(buf)

        def flush(self):
            '''
            Flush the file (upload it to S3).
            '''
            if self.isdir:
                return -errno.EISDIR
            # If it's not dirty, return
            if not self.dirty:
                return
            # Upload the file to S3
            self.bucket.upload_fileobj(
                Fileobj=self.file,
                Key=self.obj.key,
                ExtraArgs=dict(
                    Metadata=dict(
                        s3fs=dict(
                            isdir=False,
                            dirty=False
                        )
                    )
                )
            )
            # Now, get the latest version of the object
            versions = list(self.bucket.object_versions.filter(Prefix=self.obj.key))
            versions.sort(key=lambda x: x.last_modified)
            self.version = versions[-1]
            # Update the version
            self.versions[self.obj.key] = self.version.id
            # Mark the file as not dirty
            self.dirty = False
        
        def release(self, flags):
            '''
            Release the file.
            '''
            if self.isdir:
                return -errno.EISDIR
            # Flush if we haven't already
            self.flush()
        
        def ftruncate(self, size):
            '''
            Truncate the file.
            '''
            if self.isdir:
                return -errno.EISDIR
            # Truncate the file
            self.file.truncate(size)
            # Mark the file as dirty
            self.dirty = True

    def premain(self):
        # Generate cache directory if it doesn't exist
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir, exist_ok=True)
        
        # Parse config file
        config = configparser.ConfigParser()
        with open(os.path.expanduser(self.config_file), 'r') as f:
            config.read_file(f)
        if config['default']['host_base'] != config['default']['host_bucket']:
            raise Exception('host_base and host_bucket must be the same')

        client_args = dict(
            service_name='s3',
            #region_name = None,
            #api_version = None,
            use_ssl = config['default'].getboolean('check_ssl_hostname'),
            verify = config['default'].getboolean('check_ssl_certificate'),
            endpoint_url = f'''{'https' if config['default']['use_https'] else 'http'}://{config['default']['host_base']}/''',
            aws_access_key_id = config['default']['access_key'],
            aws_secret_access_key = config['default']['secret_key'],
            #aws_session_token = None,
            config = botocore.client.Config(
                #region_name = None,
                #signature_version = None,
                user_agent = 'py-s3fs/0.0.1',
                connect_timeout = 60,
                read_timeout = 60,
                parameter_validation = True,
                max_pool_connections = 10,
                proxies = dict(
                    http = config['default']['proxy_host'] + ':' + config['default']['proxy_port'],
                    https = config['default']['proxy_host'] + ':' + config['default']['proxy_port'],
                ) if config['default'].getint('proxy_port') != 0 else dict(),
                proxies_config = None,
                s3 = dict(
                    use_accelerate_endpoint = False, # TODO: Test for accelerate support
                    payload_signing_enabled = False, # Not needed
                    addressing_style = 'path', # Use path style
                ),
                retries = dict(
                    max_attempts = 5,
                    mode = 'adaptive'
                ),
                #client_cert = None,
                inject_host_prefix = False, # TODO: Test for host prefix support
                use_dualstack_endpoint = False, # TODO: Test for dualstack support
                use_fips_endpoint = False, # TODO: Test for fips support
                ignore_configured_endpoint_urls = True,
                tcp_keepalive = True,
                request_min_compression_size_bytes = 1000000, # Compress anything over 1MB
                disable_request_compression = False
            )
        )
        self.s3 = boto3.resource(**client_args)
        self.s3.meta.client.meta.events.unregister('before-sign.s3', botocore.utils.fix_s3_host)

        self.filesystem = dict()
        self.filesystem['/'] = self.S3Path(self.s3, '/', self.cache_dir)

        # Load information about all buckets
        for bucket in self.s3.buckets.all():
            bucket_path = _normpath(bucket.name)
            self.filesystem[bucket_path] = self.S3Path(self.s3, bucket_path, self.cache_dir)
            for obj in bucket.objects.all():
                obj_path = _normpath(os.path.join(bucket_path, _relnormpath(obj.key)))
                self.filesystem[obj_path] = self.S3Path(self.s3, obj_path, self.cache_dir)
    
    def __getattribute__(self, attr):
        super_getattribute = super().__getattribute__  # Cache super method for reuse
        
        try:
            attrval = super_getattribute(attr)  # Try to get attribute value
        except AttributeError:
            # See if the attribute exists in the S3Path class
            if attr not in super_getattribute('_attrs') or not hasattr(super_getattribute('S3Path'), attr):
                raise  # Re-raise the AttributeError if not handled
            def func(path, *args, **kwargs):
                # Define a function to handle attribute access on non-existing attributes
                path = _normpath(path)
                filesystem = super_getattribute('filesystem')
                if path not in filesystem:
                    return -errno.ENOENT  # Return error if path doesn't exist
                return getattr(filesystem[path], attr)(*args, **kwargs)  # Get attribute from filesystem
            attrval = func  # Set attrval to func for wrapping below
        
        # Debug wrapper for method calls if debug is enabled
        if super_getattribute('debug') and isinstance(attrval, types.MethodType) and attr in super_getattribute('_attrs'):
            def wrapper(*args, **kwargs):
                # Wrap the method call with debug print
                arg_reprs = [repr(arg) for arg in args] + [f'{key}={repr(value)}' for key, value in kwargs.items()]
                print(f'{attr}({", ".join(arg_reprs)})')
                return attrval(*args, **kwargs)
            return wrapper
        
        return attrval  # Return the attribute value or wrapped method

    def create(self, path, mode, fi=None):
        '''
        Create a file.
        '''
        path = _normpath(path)
        exists = path in self.filesystem
        if exists:
            return -errno.EEXIST
        self.filesystem[path] = self.S3Path(self.s3, path, self.cache_dir, create={
            'isdir': False,
        })
    
    def mkdir(self, path, mode):
        '''
        Create a directory.
        '''
        path = _normpath(path)
        exists = path in self.filesystem
        if exists:
            return -errno.EEXIST
        self.filesystem[path] = self.S3Path(self.s3, path, self.cache_dir, create={
            'isdir': True,
        })
    
    def link(self, target, source):
        '''
        Create a hard link.

        (Due to how S3 works, this is implemented as a copy)
        '''
        target = _normpath(target)
        source = _normpath(source)
        exists_target = target in self.filesystem
        exists_source = source in self.filesystem
        if not exists_source:
            return -errno.ENOENT
        if exists_target:
            return -errno.EEXIST
        # Copy the file
        self.filesystem[target] = self.S3Path(self.s3, target, self.cache_dir, create={
            'isdir': self.filesystem[source].isdir,
        })
        self.filesystem[target].file.write(self.filesystem[source].file.read())
        self.filesystem[target].file.flush()
    
    def unlink(self, path):
        '''
        Delete a file.
        '''
        path = _normpath(path)
        exists = path in self.filesystem
        if not exists:
            return -errno.ENOENT
        if self.filesystem[path].isdir:
            return -errno.EISDIR
        self.filesystem[path].obj.delete() # Delete the object from S3
        del self.filesystem[path]
    
    def rmdir(self, path):
        '''
        Delete a directory.
        '''
        path = _normpath(path)
        exists = path in self.filesystem
        if not exists:
            return -errno.ENOENT
        if not self.filesystem[path].isdir:
            return -errno.ENOTDIR
        del self.filesystem[path]
    
    def rename(self, old, new):
        '''
        Rename a file or directory.
        '''
        # Implemented hackily: copy the file, then delete the old file
        old = _normpath(old)
        new = _normpath(new)
        exists_old = old in self.filesystem
        exists_new = new in self.filesystem
        if not exists_old:
            return -errno.ENOENT
        if exists_new:
            return -errno.EEXIST
        # Copy the file
        self.filesystem[new] = self.S3Path(self.s3, new, self.cache_dir, create={
            'isdir': self.filesystem[old].isdir,
        })
        self.filesystem[new].file.write(self.filesystem[old].file.read())
        self.filesystem[new].file.flush()
        # Delete the old file
        self.unlink(old)
