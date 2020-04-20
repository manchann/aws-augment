import boto3
from PIL import Image, ImageFilter
import json
import time
import decimal

return_bucket_name = 'aug-module'

TMP = "/tmp/"


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def blur(image, file_name):
    path = TMP + "blur-" + file_name
    image = image.convert('RGB')
    img = image.filter(ImageFilter.BLUR)
    img.save(path)
    return path


def augmentation(file_name, image_path):
    image = Image.open(image_path)
    ret = blur(image, file_name)
    return ret


def handler(event, context):
    records = json.loads(event['Records'][0]['Sns']['Message'])
    s3_time = 0
    aug_time = 0
    image_name = ''
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
        image_name = object_path
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
    table = dynamodb.Table('lambdas')

    response = table.put_item(
        Item={
            'type': 'blur',
            'name': image_name,
            'details': {
                's3_time': decimal.Decimal(s3_time),
                'aug_time': decimal.Decimal(aug_time),
            }
        }
    )
    print('s3_time: ', s3_time)
    print('aug_time: ', aug_time)
    print('blur')
    return 'blur'
