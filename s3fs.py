from betterfuse import FuseApplication, FuseOption
import os, stat, errno, inspect, datetime, boto3, botocore, configparser, fuse

class S3Fs(FuseApplication):
    '''
    A filesystem that connects to an S3 account and exposes its buckets, objects, and configuration.
    '''

    config_path: str = FuseOption(default='~/.aws/config')
    '''
    The path to the AWS config file.
    '''

    cache_dir: str = FuseOption(default='/tmp/s3fs')
    '''
    The path to the cache directory.
    '''

    timestamp: int = datetime.datetime.now()
    '''
    The timestamp of when the filesystem was mounted, in seconds since the epoch.
    '''

    s3 = None
    '''
    The boto3 S3 resource.
    '''

    buckets: dict = dict()
    '''
    A map of bucket names to bucket objects.
    '''

    objects: dict = dict()
    '''
    A tree of object paths to object objects.

    If an object is a directory, then the directory listing will override the object.
    '''

    versions: dict = dict()
    '''
    A dict of object paths to object versions.
    '''

    offsets: dict = dict()
    '''
    A dict of object paths to sets of tuples of downloaded offsets.
    '''

    def preAttach(self):
        # Generate cache directory if it doesn't exist
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir, exist_ok=True)
        
        # Parse config file
        config = configparser.ConfigParser()
        with open(os.path.expanduser(self.config_path), 'r') as f:
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

        # Load information about all buckets
        for bucket in self.s3.buckets.all():
            # Only add buckets with versioning enabled
            if bucket.Versioning().status != 'Enabled':
                continue
            self.buckets[bucket.name] = bucket
        
        # Load information about all objects
        for bucket in self.buckets.values():
            for obj in bucket.objects.all():
                path = [bucket.name] + obj.key.split('/')
                if path[-1] == '':
                    path.pop()
                cur_obj = self.objects
                for path_part in path[:-1]:
                    if path_part not in cur_obj or not isinstance(cur_obj[path_part], dict):
                        cur_obj[path_part] = dict()
                    cur_obj = cur_obj[path_part]
                cur_obj[path[-1]] = obj

        # Save versions and initialize offsets
        for bucket in self.buckets.values():
            for obj in bucket.objects.all():
                path = [bucket.name] + obj.key.split('/')
                if path[-1] == '':
                    path.pop()
                if path[1] == '':
                    path.pop(1)
                self.offsets['/'.join(path)] = set()
                latest_version_time = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
                for version in bucket.object_versions.filter(Prefix=obj.key):
                    if version.last_modified > latest_version_time and version.last_modified < self.timestamp:
                        latest_version_time = version.last_modified
                        self.versions['/'.join(path)] = version.id

    def getattr(self, path):
        # Split path into a list of parts
        path_parts = path.split('/')
        if path_parts[-1] == '':
            path_parts.pop()
        path_parts.pop(0)
        # Return based on the path
        cur_obj = self.objects
        for path_part in path_parts:
            if path_part not in cur_obj:
                return -errno.ENOENT
            cur_obj = cur_obj[path_part]
        st = fuse.Stat()
        # If it's a directory, return a directory
        if isinstance(cur_obj, dict):
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_nlink = 2 + len(cur_obj)
            return st
        # If it's an object, return a file
        st.st_mode = stat.S_IFREG | 0o444
        st.st_nlink = 1
        st.st_size = cur_obj.size
        return st

    def readdir(self, path, offset):
        # Split path into a list of parts
        path_parts = path.split('/')
        if path_parts[-1] == '':
            path_parts.pop()
        path_parts.pop(0)
        # Return based on the path
        cur_obj = self.objects
        for path_part in path_parts:
            if path_part not in cur_obj:
                return -errno.ENOENT
            cur_obj = cur_obj[path_part]
        # If it's a directory, return a directory
        if isinstance(cur_obj, dict):
            yield fuse.Direntry('.')
            yield fuse.Direntry('..')
            for obj_name in cur_obj.keys():
                yield fuse.Direntry(obj_name)
            return
        # If it's an object, return a file
        yield fuse.Direntry(cur_obj.key.split('/')[-1])

    def open(self, path, flags):
        # Split path into a list of parts
        path_parts = path.split('/')
        if path_parts[-1] == '':
            path_parts.pop()
        path_parts.pop(0)
        # Return based on the path
        cur_obj = self.objects
        for path_part in path_parts:
            if path_part not in cur_obj:
                return -errno.ENOENT
            cur_obj = cur_obj[path_part]
        # If it's a directory, return EISDIR
        if isinstance(cur_obj, dict):
            return -errno.EISDIR
        # If it's an object, return a file and check permissions
        accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
        if (flags & accmode) != os.O_RDONLY:
            return -errno.EACCES

    def read(self, path, size, offset):
        # Split path into a list of parts
        path_parts = path.split('/')
        if path_parts[-1] == '':
            path_parts.pop()
        path_parts.pop(0)
        path_rel = '/'.join(path_parts)
        # Return based on the path
        cur_obj = self.objects
        for path_part in path_parts:
            if path_part not in cur_obj:
                return -errno.ENOENT
            cur_obj = cur_obj[path_part]
        # If it's a directory, return EISDIR
        if isinstance(cur_obj, dict):
            return -errno.EISDIR
        # If it's an object, return a file
        if offset >= cur_obj.size:
            return b''
        if offset + size > cur_obj.size:
            size = cur_obj.size - offset
        # Get sections of file that haven't been downloaded yet
        sections = [(offset, offset + size)]
        retry = True
        while retry:
            retry = False
            for section_have in self.offsets[path_rel]:
                for i in range(len(sections)):
                    section_want = sections[i]
                    # If the section is already downloaded, remove it from the list
                    if section_have[0] >= section_want[0] and section_have[1] <= section_want[1]:
                        sections.pop(i)
                        retry = True
                        break
                    # If the section is partially downloaded from the start, truncate it
                    if section_have[0] >= section_want[0] and section_have[0] < section_want[1]:
                        sections[i] = (section_have[1], section_want[1])
                        retry = True
                        break
                    # If the section is partially downloaded from the end, truncate it
                    if section_have[1] > section_want[0] and section_have[1] <= section_want[1]:
                        sections[i] = (section_want[0], section_have[0])
                        retry = True
                        break
                    # If the section is partially downloaded from the middle, split it
                    if section_have[0] > section_want[0] and section_have[1] < section_want[1]:
                        sections[i] = (section_want[0], section_have[0])
                        sections.insert(i + 1, (section_have[1], section_want[1]))
                        retry = True
                        break
                if retry:
                    break
        # Create file and directories if it doesn't exist
        if not os.path.exists(os.path.join(self.cache_dir, path_rel)):
            os.makedirs(os.path.dirname(os.path.join(self.cache_dir, path_rel)), exist_ok=True)
            with open(os.path.join(self.cache_dir, path_rel), 'bw') as f:
                f.truncate(cur_obj.size)
        # Download sections of file that haven't been downloaded yet
        with open(os.path.join(self.cache_dir, path_rel), 'br+') as f:
            for section in sections:
                f.seek(section[0])
                f.write(cur_obj.get(Range=f'''bytes={section[0]}-{section[1]}''', VersionId=self.versions[path_rel]).get('Body').read())
            f.flush()

            f.seek(offset)
            retval = f.read(size)
        
        # Add downloaded sections to the list
        for section in sections:
            self.offsets[path_rel].add(section)
        # Remove contiguous sections from the list
        for i in range(len(self.offsets[path_rel])):
            for j in range(i + 1, len(self.offsets[path_rel])):
                if self.offsets[path_rel][i][1] == self.offsets[path_rel][j][0]:
                    self.offsets[path_rel][i] = (self.offsets[path_rel][i][0], self.offsets[path_rel][j][1])
                    self.offsets[path_rel].pop(j)
                    break
        
        return retval

