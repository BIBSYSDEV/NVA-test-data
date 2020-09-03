import boto3
import json
import copy
import uuid
from os import listdir

dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')
publication_template_file_name = './publications/publication.json'
test_publications_file_name = './publications/test_publications.json'
publications_tablename = 'nva_resources'
s3_bucket_name = 'nva-s3-storage-bucket-nvastoragebucket-1vc7h4ulsm9bj'

def delete_files_from_s3():
    s3_objects = s3_client.list_objects_v2(Bucket=s3_bucket_name)

    if s3_objects['KeyCount'] > 0:
        object_keys = []
        for s3_object in s3_objects['Contents']:
            object_keys.append({
                'Key': s3_object['Key']
            })

        s3_client.delete_objects(Bucket=s3_bucket_name, Delete = {
            'Objects': object_keys
        })    
    return

def add_files_to_s3():
    files = listdir('./publications/files')
    for import_file in files:
        key = import_file.split('.')[0]
        with open('./publications/files/{}'.format(import_file),'rb') as import_file_body:
            s3_client.put_object(Bucket=s3_bucket_name, Key=key, Body=import_file_body, 
                ContentDisposition='filename="{}"'.format(import_file), ContentType='text/plain')
    return

def scan_publications():
    response = dynamodb_client.scan(TableName=publications_tablename)

    return response['Items']

def delete_publications():
    publications = scan_publications()
    for publication in publications:
        identifier = publication['identifier']['S']
        if identifier.startswith('test_'):
            response = client.delete_item(
                TableName=publication_tablename,
                Key={'identifier': {
                    'S': identifier
                }})
    return


def put_item(new_publication):

    response = dynamodb_client.put_item(TableName=publications_tablename,
                               Item=new_publication)
    return response

def create_publications():
    with open(publication_template_file_name) as publication_template_file:
        publication_template = json.load(publication_template_file)

    with open(test_publications_file_name) as test_publications_file:

        test_publications = json.load(test_publications_file)
        for test_publication in test_publications:
            new_publication = copy.deepcopy(publication_template)
            new_publication['publicationType']['S'] = test_publication['publication_type']
            new_publication['entityDescription']['M']['reference']['M']['publicationContext']['M']['type']['S'] = test_publication['publication_context_type']
            new_publication['entityDescription']['M']['reference']['M']['publicationInstance']['M']['type']['S'] = test_publication['publication_instance_type']
            new_publication['fileSet']['M']['files']['L'][0]['M']['identifier']['S'] = test_publication['file_identifier']
            new_publication['fileSet']['M']['files']['L'][0]['M']['name']['S'] = test_publication['file_name']
            new_publication['owner']['S'] = test_publication['owner']
            new_publication['status']['S']= test_publication['status']

            result = put_item(new_publication)


def run():
    print('publications...')
    delete_files_from_s3()
    add_files_to_s3()

    delete_publications()
    create_publications()
