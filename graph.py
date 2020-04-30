import boto3
import matplotlib

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2')
table = dynamodb.Table('lambda')

response = table.scan()

start_time = 9999999999999.0
start_time_max = 0
end_time = 0
num = 0
for res in response['Items']:
    start_time = min(start_time, res['details']['start_time'])
    start_time_max = max(start_time_max, res['details']['start_time'])
    end_time = max(end_time, res['details']['end_time'])
    num += 1
    print(res)

print('총 걸린 시간:', end_time - start_time)
print(start_time_max - start_time)
print('이미지 개수:', num)
