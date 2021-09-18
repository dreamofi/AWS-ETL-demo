import json
import boto3
import os
import uuid

SFN = boto3.client('stepfunctions')

def lambda_handler(event, context):

    '''
    This function start AWS Step Functions
    '''
    print(json.dumps(event, indent=3))
    try:
        bucket_name = event["Records"][0]['s3']['bucket']['name'] 
        bucket_arn = event["Records"][0]['s3']['bucket']['arn']
        key_name = event["Records"][0]['s3']['object']['key']
        file_name = event["Records"][0]['s3']['object']['key'].split('/')[-1]
        if os.path.splitext(file_name)[1] != ".csv":
            raise Exception("Not csv file")
        if len(event["Records"][0]['s3']['object']['key'].split('/')) != 3:
            raise Exception("File must be put in a schema folder")
        schema_name = key_name.split('/')[1]
        state_machine_arn = "arn:aws:states:us-west-1:541253215789:stateMachine:de-etl-manual-dummy"
        trigger_pipeline_by_key(state_machine_arn, bucket_name, bucket_arn, key_name, file_name, schema_name)
        print(bucket_name)
        print(key_name)
        print(file_name)
        print(schema_name)
        print("Done")
    except Exception as e:
        raise e
        
def trigger_pipeline_by_key(sf_arn, bucket_name, bucket_arn, key_name, file_name, schema_name):
    inpt = {    
    'bucket_name': bucket_name,
    'bucket_arn': bucket_arn,
    'key_name': key_name,
    'file_name': file_name,
    'schema_name': schema_name,
    }
    response = SFN.start_execution(stateMachineArn = sf_arn, input = json.dumps(inpt))
    return response