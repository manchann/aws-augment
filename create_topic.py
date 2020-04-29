import boto3
import json
import os
import subprocess

bucket_name = 'pre-image-group'

bucket = boto3.resource('s3').Bucket(bucket_name)

ret_arr = []
num = 0
dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
table = dynamodb.Table('lambda')

scan = table.scan()
with table.batch_writer() as batch:
    for each in scan['Items']:
        batch.delete_item(Key={
            'id': each['id'],
            'type': each['type']
        })
for bucket_object in bucket.objects.all():
    ret = {}
    ret['bucket_name'] = bucket_name
    ret['object_path'] = bucket_object.key
    subprocess.check_call(
        "aws sns publish --topic-arn arn:aws:sns:ap-northeast-2:060127333571:lambda-augment --message '{}' &".format(
            json.dumps(ret)),
        shell=True)
    num += 1
print('이미지 개수:', num)
