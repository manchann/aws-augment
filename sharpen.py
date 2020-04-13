import boto3
from PIL import Image, ImageFilter
import json
import time

return_bucket_name = 'aug-module'

TMP = "/tmp/"


def sharpen(image, file_name):
    path = TMP + "sharpen-" + file_name
    image = image.convert('RGB')
    img = image.filter(ImageFilter.SHARPEN)
    img.save(path)
    return path


def augmentation(file_name, image_path):
    image = Image.open(image_path)
    ret = sharpen(image, file_name)
    return ret


def handler(event, context):
    records = json.loads(event['Records'][0]['Sns']['Message'])
    s3_time = 0
    aug_time = 0
    for record in records['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_path = record['s3']['object']['key']
        tmp = '/tmp/' + object_path
        s3_start = time.time()
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, object_path, tmp)
        s3_end = time.time()
        s3_time += s3_end - s3_start
        aug_start = time.time()
        ret = augmentation(object_path, tmp)
        aug_end = time.time()
        aug_time += aug_end - aug_start
        s3.upload_file(ret, return_bucket_name, ret.split('/')[2])
    print('s3_time: ', s3_time)
    print('aug_time: ', aug_time)
    print('sharpen')
    return 'sharpen'
