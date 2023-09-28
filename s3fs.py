from betterfuse import FuseApplication, FuseOption
import os, stat, errno, inspect, datetime, boto3, botocore, configparser, fuse, io, types
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
                    self.fd = os.open(os.path.join(self.cache_dir, _relnormpath(path)), os.O_RDWR)
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
        
    def __getattr__(self, meth):
        if meth not in self._attrs or not hasattr(self.S3Path, meth):
            return super().__getattr__(meth)
        enoent = meth in {'getattr', 'readdir', 'open', 'read', 'write', 'flush'}
        eexist = meth in {'mkdir'}
        isdir = meth in {'mkdir', 'readdir'}
        def func(path, *args, **kwargs):
            path = _normpath(path)
            exists = path in self.filesystem
            if not exists:
                if enoent:
                    return -errno.ENOENT
                else:
                    self.filesystem[path] = self.S3Path(self.s3, path, self.cache_dir, create={
                        'isdir': isdir,
                    })
            elif eexist:
                return -errno.EEXIST
            #print(f'''{meth}({path}, {args}, {kwargs})''')
            return getattr(self.filesystem[path], meth)(*args, **kwargs)
        return func
