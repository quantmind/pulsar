"""Upload package related data to S3
"""
import os
import glob
from distutils.cmd import Command
from distutils.errors import DistutilsModuleError, DistutilsOptionError


class S3Data(Command):
    description = 'Upload package related data to S3'

    user_options = [
        ('bucket=', None, 'S3 bucket to upload to'),
        ('download', None, 'Download data from S3'),
        ('base-key=', None, 'Base key'),
        ('key=', None, 'Key for upload'),
        ('files=', None, 'List of files or glob to upload (comma separated)')
    ]

    def initialize_options(self):
        self.bucket = ''
        self.base_key = 'python-packages'
        self.key = 'data'
        self.files = None
        self.download = False

    def finalize_options(self):
        files = [s.strip() for s in (self.files or '').split(',') if s.strip()]
        self.files = files

    def run(self):
        s3 = self.client()
        meta = self.distribution.metadata
        bucket = self.bucket
        vkey = meta.version.replace('.', '_')
        key = '%s/%s/%s/%s' % (self.base_key, meta.name, vkey, self.key)

        if not bucket:
            raise DistutilsOptionError('bucket is required')

        if not self.files and not self.download:
            raise DistutilsOptionError(
                'No files given nor download flag passed'
            )

        if self.download:
            return self.download_from(s3, bucket, key)

        for pattern in self.files:
            for file in glob.iglob(pattern):
                target = '%s/%s' % (key, os.path.basename(file))
                with open(file, 'rb') as fp:
                    body = fp.read()
                    s3.put_object(
                        Body=body,
                        Bucket=bucket,
                        Key=target
                    )
                    self.announce(
                        'Uploaded %s to %s::%s' % (file, bucket, target), 2
                    )

    def download_from(self, s3, bucket, key):
        pkg_dir = os.path.abspath(self.distribution.package_dir or os.curdir)
        loc = os.path.join(pkg_dir, self.key)
        if os.path.isdir(loc):
            raise DistutilsOptionError(
                'Cannot download to %s, directory already available', loc
            )
        response = s3.list_objects(Bucket=bucket, Prefix=key)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            os.mkdir(loc)

            for key in response['Contents']:
                key = key['Key']
                file = key.split('/')[-1]
                target = os.path.join(loc, file)
                response = s3.get_object(
                    Bucket=bucket,
                    Key=key
                )
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    with open(target, 'wb') as fp:
                        fp.write(response['Body'].read())

                    self.announce(
                        'Downloaded %s::%s to %s' % (bucket, key, target), 2
                    )

    def client(self):
        try:
            from botocore.session import get_session
        except ImportError:
            raise DistutilsModuleError('botocore is required')
        return get_session().create_client('s3')
