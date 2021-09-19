import json
import pandas as pd
from cerberus import Validator
from datetime import datetime
import boto3
import os


def lambda_handler(event, context):
    '''
    This function validates input raw dataset against schema in the schema folder
    '''

    print(event)
    result = {}
    s3_resource = boto3.resource('s3')
    # to_date = lambda s: datetime.strptime(s, os.environ['dateformat'])
    bucket_name = event['bucket_name']
    key_name = event['key_name']
    source_file_name = event['file_name']
    schema_file_name = event['schema_name'] + '.json'
    schema_folder = "/" + str(os.environ['schema_folder_name']) + "/"

    # source_file_name_to_copy = bucket_name + "/" + key_name
    # error_file_name="error/" + source_file_name

    # Read schema
    schema_file_path = "s3://" + bucket_name + schema_folder + schema_file_name

    try:
        schema = json.loads(s3_resource.Object(
            bucket_name, "schema/" + schema_file_name).get()['Body'].read().decode())
        print("Successfuly read : " + schema_file_path)
    except:
        result['Validation'] = "FAILURE"
        result['Reason'] = "Errro while reading schema"
        result['Location'] = os.environ["schema_folder_name"]
        print("Error while reading schema")
        return(result)

    for keys in schema:
        if 'format' in schema[keys]:
            date_format_provided = schema[keys]['format']
            def to_date(s): return datetime.strptime(s, date_format_provided)
            schema[keys].pop("format")
            schema[keys]['coerce'] = to_date

    v = Validator(schema)
    v.allow_unknown = False
    v.require_all = True

    # Read dataset file
    source_file_path = "s3://" + bucket_name + "/" + key_name
    try:
        df = pd.read_csv(source_file_path)
        print("Successfuly read : " + source_file_path)
    except:
        result['Validation'] = "FAILURE"
        result['Reason'] = "Errro while reading csv"
        result['Location'] = os.environ['source_folder_name'] 
        print("Error while reading csv")
        return(result)

    result['Validation'] = "SUCCESS"
    result['Location'] = "source"
    df_dict = df.to_dict(orient='records')

    transformed_file_name = "s3://" + bucket_name + "/" + \
        str(os.environ['stage_folder_name']) + "/" + source_file_name

    if len(df_dict) == 0:
        result['Validation'] = "FAILURE"
        result['Reason'] = "NO RECORD FOUND"
        result['Location'] = os.environ['source_folder_name']
        print("Moving file to error folder")
        return(result)
    for idx, record in enumerate(df_dict):
        if not v.validate(record):
            result['Validation'] = "FAILURE"
            result['Reason'] = str(v.errors) + " in record number " + str(idx)
            result['Location'] = os.environ['source_folder_name']
            print("Moving file to error folder")
            return(result)
            break

    try:
        df['Month'] = df['Date'].astype(str).str[0:2]
        df['Day'] = df['Date'].astype(str).str[3:5]
        df['Year'] = df['Date'].astype(str).str[6:10]
        #Big file is very likely to DIE!!
        df.to_csv(transformed_file_name, index=False)
        check = s3_resource.Object(bucket_name, key_name).delete()
    except:
        result['Validation'] = "FAILURE"
        result['Reason'] = "Can't save to stage"
        result['Location'] = os.environ['stage_folder_name']
        print("check", check)
        return result
    print("Successfuly moved file to  : " + transformed_file_name)
    return(result)
