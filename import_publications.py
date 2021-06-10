import boto3
import json
import copy
import uuid
import requests
import uuid
import os
import common

dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')
ssm = boto3.client('ssm')
publications_tablename = ssm.get_parameter(Name='/test/RESOURCE_TABLE',
                                           WithDecryption=False)['Parameter']['Value']
s3_bucket_name = ssm.get_parameter(Name='/test/RESOURCE_S3_BUCKET',
                                   WithDecryption=False)['Parameter']['Value']
STAGE = ssm.get_parameter(Name='/test/STAGE',
                          WithDecryption=False)['Parameter']['Value']
USER_POOL_ID = ssm.get_parameter(Name='/test/AWS_USER_POOL_ID',
                                 WithDecryption=False)['Parameter']['Value']
CLIENT_ID = ssm.get_parameter(Name='/test/AWS_USER_POOL_WEB_CLIENT_ID',
                              WithDecryption=False)['Parameter']['Value']
publication_template_file_name = './publications/new_test_registration.json'
test_publications_file_name = './publications/test_publications.json'
user_tablename = 'UsersAndRolesTable'
person_query = 'https://api.{}.nva.aws.unit.no/person/?name={} {}'
user_endpoint = 'https://api.{}.nva.aws.unit.no/users-roles/users/{}'
upload_endpoint = 'https://api.{}.nva.aws.unit.no/upload/{}'
publication_endpoint = 'https://api.{}.nva.aws.unit.no/publication'.format(STAGE)
upload_create = upload_endpoint.format(STAGE, 'create')
upload_prepare = upload_endpoint.format(STAGE, 'prepare')
upload_complete = upload_endpoint.format(STAGE, 'complete')
username = 'test-data-user@test.no'
test_file_name = 'publications/files/test_file.pdf'
test_file_size = os.stat(test_file_name).st_size
test_file_modified = os.stat(test_file_name).st_mtime
test_file = open(test_file_name, 'rb').read()


arp_dict = {}
file_dict = {}

def map_user_to_arp():
    with open('./users/test_users.json') as user_file:
        users = json.load(user_file)
        for user in users:
            arp_dict[user['username']] = {
                'familyName': user['familyName'],
                'givenName': user['givenName']
            }
            if (user['author']):
                query_response = requests.get(
                    person_query.format(STAGE, user['givenName'],
                                        user['familyName']))
                if query_response.status_code != 200:
                    print('GET /person/ {}'.format(query_response.status_code))
                if query_response.json() != []:
                    arp_dict[user['username']]['scn'] = query_response.json(
                    )[0]['id']

def upload_file(bearer_token):
    print('upload file...')
    headers = {
      'Authorization': 'Bearer {}'.format(bearer_token),
      'accept': 'application/pdf'
    }
    # create
    print('create...')
    response = requests.post(
      upload_create, 
      json={
        'filename': 'test_file.pdf',
        'size': 32404,
        'lastmodified': 1353189358000,
        'mimetype': 'application/pdf'
      }, 
      headers=headers)
    uploadId = response.json()['uploadId']
    key = response.json()['key']
    # prepare
    response = requests.post(
      upload_prepare,
      json={
        'number': '1',
        'uploadId': uploadId,
        'body': str(test_file),
        'key': key
      },
      headers=headers)
    presignedUrl = response.json()['url']
    # upload
    response = requests.put(presignedUrl, headers = { 'Accept': 'appliation/pdf' }, data = test_file)
    ETag = response.headers['ETag']
    # complete
    print('complete...')
    response = requests.post(
      upload_complete,
      json={
        'uploadId': uploadId,
        'key': key,
        'parts': [
          {
            'partNumber': '1',
            'Etag': ETag
          }
        ]
      },
      headers=headers)
    return

def scan_resources():
    print('scanning resourcess')
    response = dynamodb_client.scan(TableName=publications_tablename)
    scanned_publications = response['Items']
    more_items = 'LastEvaluatedKey' in response
    while more_items:
        start_key = response['LastEvaluatedKey']
        response = dynamodb_client.scan(TableName=publications_tablename, ExclusiveStartKey=start_key)
        scanned_publications.extend(response['Items'])
        more_items = 'LastEvaluatedKey' in response
    return scanned_publications


def delete_publications():
    resources = scan_resources()
    for resource in resources:
        if resource['type']['S'] == 'Resource':
            publication = resource['data']['M']
            primary_partition_key = resource['PK0']['S']
            primary_sort_key = resource['SK0']['S']
            identifier = publication['identifier']['S']
            owner = publication['owner']['S']
            if 'test.no' in owner:
                print(
                    'Deleting {} - {}'.format(identifier, owner))
                try:
                    response = dynamodb_client.delete_item(
                        TableName=publications_tablename,
                        Key={
                            'PK0': {
                                'S': primary_partition_key
                            },
                            'SK0': {
                                'S': primary_sort_key
                            }
                        })
                except e:
                    print(e)
    return


def put_item(new_publication, bearer_token):
    headers = {
      'Authorization': 'Bearer {}'.format(bearer_token),
      'accept': 'application/json'
    }
    response = requests.post(publication_endpoint, json=new_publication, headers=headers)


def get_customer(username, bearer_token):
    headers = {
      'Authorization': 'Bearer {}'.format(bearer_token),
      'accept': 'application/json'
    }
    response = requests.get(user_endpoint.format(STAGE, username), headers=headers)
    return response.json()['institution']


def create_contributor(contributor):
    with open('./publications/contributors.json'
                ) as contributor_template_file:
        contributor_template = json.load(contributor_template_file)

        new_contributor = copy.deepcopy(contributor_template)
        new_contributor['email'] = contributor
        new_contributor['identity']['id'] = arp_dict[contributor]['scn']
        new_contributor['identity']['name'] = '{},{}'.format(
                arp_dict[contributor]['familyName'],
                arp_dict[contributor]['givenName'])
        return new_contributor


def create_publication_data(publication_template, test_publication, username, customer, status):
    new_publication = copy.deepcopy(publication_template)
    new_publication['entityDescription']['mainTitle'] = test_publication['title']
    new_publication['entityDescription']['reference']['publicationContext']['type'] = test_publication['publication_context_type']
    new_publication['entityDescription']['reference']['publicationInstance']['type'] = test_publication['publication_instance_type']
    new_publication['owner'] = username
    new_publication['publisher']['id'] = customer
    new_publication['status'] = status

    if test_publication['contributor'] != '':
        contributor = test_publication['contributor']
        new_contributor = create_contributor(contributor=contributor)
        new_publication['entityDescription']['contributors'].append(new_contributor)

    return new_publication

def create_test_publication(publication_template, test_publication, bearer_token):
    customer = get_customer(test_publication['owner'], bearer_token=bearer_token).replace('https://api.dev.nva.aws.unit.no/customer/', '')
    username = test_publication['owner']
    status = test_publication['status']

    new_publication = create_publication_data(
        publication_template=publication_template,
        test_publication=test_publication,
        username=username,
        customer=customer,
        status=status
    )

    return new_publication

def create_publications(bearer_token):
    with open(publication_template_file_name) as publication_template_file:
        publication_template = json.load(publication_template_file)

    with open(test_publications_file_name) as test_publications_file:

        test_publications = json.load(test_publications_file)
        for test_publication in test_publications:

            new_publication = create_test_publication(
                publication_template=publication_template,
                test_publication=test_publication,
                bearer_token=bearer_token
            )
            print(test_publication['title'])
            put_item(new_publication=new_publication, bearer_token=bearer_token)


def run():
    print('publications...')
    bearer_token = common.login()
    map_user_to_arp()
    # upload_file(bearer_token)

    delete_publications()
    create_publications(bearer_token=bearer_token)


if __name__ == '__main__':
    run()
