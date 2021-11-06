import json
import boto3
from datetime import datetime
from datetime import timedelta
import configparser
import hashlib

def upperIndex(logs,end_timestamp):
    index = -1
    start = 0
    end = len(logs)-1
    while start <= end:
        mid = (start+end)//2
        curr_timestamp = logs[mid].split(" ")[0].split(".")[0]
        if curr_timestamp == end_timestamp:
            index = mid
            start = mid + 1
        elif curr_timestamp > end_timestamp:
            end = mid - 1
        else:
            start = mid + 1
    print("end_timestamp ",end_timestamp)
    print("curr_timestamp ",curr_timestamp)
    print("mid", mid)
    if mid == 0:
        return -1
    else:
        return mid


def lowerIndex(logs,start_timestamp):
    index = -1
    start = 0
    end = len(logs)-1
    while start <= end:
        mid = (start+end)//2
        curr_timestamp = logs[mid].split(" ")[0].split(".")[0]
        if curr_timestamp == start_timestamp:
            index = mid
            end = mid - 1
        elif curr_timestamp > start_timestamp:
            end = mid - 1
        else:
            start = mid + 1
    print("start_timestamp ",start_timestamp)
    print("curr_timestamp ",curr_timestamp)
    print("mid", mid)
    if mid == 0 or mid == len(logs)-1:
        return -1 
    else:
        return mid

def lambda_handler(event, context):
    config = configparser.ConfigParser()
    config.read('config.ini')
    bucket = config['DEFAULT']['S3_BUCKET']
    key = config['DEFAULT']['S3_KEY']
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key) #Enter bucket name and key(file name)
    data = response['Body'].read().decode('utf-8')
    logs = data.split("\n")


    #1. Parse out query string params
    print(event)
    inputTime = event['queryStringParameters']['T']
    inputDeltaTime = event['queryStringParameters']['dT']
    print('transactionTime=',inputTime)
    print('transactionDeltaTime=',inputDeltaTime)

    #2. Add Timestamps
    time_format_str = '%H:%M:%S'
    initialTime = datetime.strptime(inputTime,time_format_str)
    inputDeltaTimeList = inputDeltaTime.split(":")
    startDateTime = initialTime - timedelta(hours=int(inputDeltaTimeList[0]))-timedelta(minutes=int(inputDeltaTimeList[1]))-timedelta(seconds=int(inputDeltaTimeList[2]))
    endDateTime = initialTime + timedelta(hours=int(inputDeltaTimeList[0]))+timedelta(minutes=int(inputDeltaTimeList[1]))+timedelta(seconds=int(inputDeltaTimeList[2]))
    startTime = startDateTime.strftime("%H:%M:%S")
    endTime = endDateTime.strftime("%H:%M:%S")

    print("Start Time is ",startTime)
    print("End Time is ",endTime)

    #3. Calculate whether logs are present in the timestamp range
    responseObject = {}
    transactionResponse = {}
    found = False
    lower_index = lowerIndex(logs,startTime)
    upper_index = upperIndex(logs,endTime)
    print("lower_index",lower_index)
    print("upper_index",upper_index)
    if lower_index == -1 and upper_index == -1:
        responseObject['statusCode'] = 404
    elif lower_index >= upper_index:
        responseObject['statusCode'] = 404
    else:
        if upper_index == -1:
            upper_index = len(logs)
        lower_index += 1
        transactionResponse['lower_index'] = lower_index
        transactionResponse['upper_index'] = upper_index
        transactionResponse['content'] = []
        for ele in logs[lower_index:upper_index]:
            transactionResponse['content'].append(str(hashlib.md5(ele.encode())))
        responseObject['statusCode'] = 200
        found = True

    #4. Construct the body of the response object
    transactionResponse['isPresent'] = str(found)

    #5. Construct http response
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['body'] = json.dumps(transactionResponse)

    #6. Return the response Object
    return responseObject
