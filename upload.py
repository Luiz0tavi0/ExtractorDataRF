import sys
import boto3
from boto3.s3.transfer import TransferConfig
import threading


class AmazonS3:

    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.s3 = boto3.resource('s3',
                                 aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key,
                                 region_name=region_name)

    def push_data_to_s3_bucket(self, bucket_name, data, file_name, file_size, content_type):
        config = TransferConfig(multipart_threshold=1024 * 25,  # limit above which multiparts activate
                                max_concurrency=10,  # threads
                                multipart_chunksize=1024 * 25,  # size of data in each thread
                                use_threads=True)  # enabling threads

        self.s3.Object(bucket_name, file_name).upload_fileobj(data,
                                                              ExtraArgs={'ContentType': content_type},
                                                              Config=config,
                                                              Callback=self.ProgressPercentage(file_name, file_size)
                                                              )

    def show_contents_s3_bucket(self, bucket_name):
        bucket = self.s3.Bucket(bucket_name)
        print()
        print(f"Bucket : {bucket_name}")
        for obj in bucket.objects.all():
            print(f'filename : {obj.key} ')

    # def delete_contents_s3_bucket(self, bucket_name, file_name):
    #     self.s3.Object(bucket_name, file_name).delete()
    #     self.show_contents_s3_bucket(bucket_name)

    # def empty_bucket(self, bucket_name):
    #     self.s3.Bucket(bucket_name).objects.all().delete()

    class ProgressPercentage(object):
        def __init__(self, filename, size):
            self._filename = filename
            self._size = float(size)
            self._seen_so_far = 0
            self._lock = threading.Lock()

        def __call__(self, bytes_amount):
            with self._lock:
                self._seen_so_far += bytes_amount
                percentage = (self._seen_so_far / self._size) * 100
                sys.stdout.write(
                    "\r%s  %s / %s  (%.2f%%)" % (
                        self._filename, self._seen_so_far, self._size,
                        percentage))
                sys.stdout.flush()