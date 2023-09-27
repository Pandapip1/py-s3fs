#!/usr/bin/env python

from s3fs import S3Fs

if __name__ == '__main__':
    server = S3Fs()
    server.main()
