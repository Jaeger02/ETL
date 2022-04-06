import boto3
class Extract:
    __arg_date = ""
    __src_format = ''
    __src_bucket = ''
    __trg_bucket = ''
    __columns = []
    __key = ''
    __s3 = boto3.resource('s3')

