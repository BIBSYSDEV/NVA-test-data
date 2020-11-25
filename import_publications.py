import boto3
import json
import copy
import uuid
import requests
import uuid
from os import listdir

dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')
publication_template_file_name = './publications/publication.json'
test_publications_file_name = './publications/test_publications.json'
publications_tablename = 'nva_resources'
user_tablename = 'UsersAndRolesTable'
person_query = 'https://api.{}.nva.aws.unit.no/person/?name={} {}'

ssm = boto3.client('ssm')
s3_bucket_name = ssm.get_parameter(Name='/test/RESOURCE_S3_BUCKET',
                                   WithDecryption=False)['Parameter']['Value']
STAGE = ssm.get_parameter(Name='/test/STAGE',
                          WithDecryption=False)['Parameter']['Value']

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


def delete_files_from_s3():
    s3_objects = s3_client.list_objects_v2(Bucket=s3_bucket_name)

    if s3_objects['KeyCount'] > 0:
        object_keys = []
        files = listdir('./publications/files')
        for s3_object in s3_objects['Contents']:
            head = s3_client.head_object(Bucket=s3_bucket_name,
                                         Key=s3_object['Key'])

            filename = head['ContentDisposition'].replace('filename=',
                                                          '').replace('"', '')
            if filename in files:
                object_keys.append({'Key': s3_object['Key']})

        if(len(object_keys) > 0):
            s3_client.delete_objects(Bucket=s3_bucket_name,
                                     Delete={'Objects': object_keys})
    return


def add_files_to_s3():
    files = listdir('./publications/files')
    for import_file in files:
        key = str(uuid.uuid4())
        file_dict[import_file] = key
        with open('./publications/files/{}'.format(import_file),
                  'rb') as import_file_body:
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=key,
                Body=import_file_body,
                ContentDisposition='filename="{}"'.format(import_file),
                ContentType='text/plain')
    return


def scan_publications():
    print('scanning publications')
    response = dynamodb_client.scan(TableName=publications_tablename)
    return response['Items']


def delete_publications():
    publications = scan_publications()
    print(len(publications))
    for publication in publications:
        modifiedDate = publication['modifiedDate']['S']
        identifier = publication['identifier']['S']
        owner = publication['owner']['S']
        if 'test.no' in owner:
            print(
                'Deleting {} - {}'.format(publication['identifier']['S'], owner))
            response = dynamodb_client.delete_item(
                TableName=publications_tablename,
                Key={
                    'identifier': {
                        'S': identifier
                    },
                    'modifiedDate': {
                        'S': modifiedDate
                    }
                })
    return


def put_item(new_publication):

    response = dynamodb_client.put_item(TableName=publications_tablename,
                                        Item=new_publication)
    return response


def get_customer(username):
    response = dynamodb_client.get_item(TableName=user_tablename,
                                        Key={
                                            'PrimaryKeyHashKey': {
                                                'S': 'USER#{}'.format(username)
                                            },
                                            'PrimaryKeyRangeKey': {
                                                'S': 'USER#{}'.format(username)
                                            },
                                        })
    return response['Item']['institution']['S']


def create_publications():
    with open(publication_template_file_name) as publication_template_file:
        publication_template = json.load(publication_template_file)

    with open(test_publications_file_name) as test_publications_file:

        test_publications = json.load(test_publications_file)
        for test_publication in test_publications:

            customer = get_customer(test_publication['owner'])

            new_publication = copy.deepcopy(publication_template)
            new_publication['identifier']['S'] = str(uuid.uuid4())
            new_publication['entityDescription']['M']['mainTitle'][
                'S'] = test_publication['title']
            new_publication['entityDescription']['M']['reference']['M'][
                'publicationContext']['M']['type']['S'] = test_publication[
                    'publication_context_type']
            new_publication['entityDescription']['M']['reference']['M'][
                'publicationInstance']['M']['type']['S'] = test_publication[
                    'publication_instance_type']
            new_publication['fileSet']['M']['files']['L'][0]['M'][
                'identifier']['S'] = file_dict[test_publication['file_name']]
            new_publication['fileSet']['M']['files']['L'][0]['M']['name'][
                'S'] = test_publication['file_name']
            new_publication['owner']['S'] = test_publication['owner']
            new_publication['publisher']['M']['id']['S'] = customer
            new_publication['publisherId']['S'] = customer
            new_publication['publisherOwnerDate'][
                'S'] = '{}#{}#2020-01-01T00:00:00.000000Z'.format(
                    customer, test_publication['owner'])
            new_publication['status']['S'] = test_publication['status']

            if test_publication['contributor'] != '':
                contributor = test_publication['contributor']
                with open('./publications/contributors.json'
                          ) as contributor_template_file:
                    contributor_template = json.load(contributor_template_file)

                    new_contributor = copy.deepcopy(contributor_template)
                    new_contributor['M']['email']['S'] = contributor
                    new_contributor['M']['identity']['M']['arpId'][
                        'S'] = arp_dict[contributor]['scn']
                    new_contributor['M']['identity']['M']['name'][
                        'S'] = '{},{}'.format(
                            arp_dict[contributor]['familyName'],
                            arp_dict[contributor]['givenName'])

                    new_publication['entityDescription']['M']['contributors'][
                        'L'].append(new_contributor)

            put_item(new_publication)


def run():
    print('publications...')
    map_user_to_arp()
    delete_files_from_s3()
    add_files_to_s3()

    delete_publications()
    create_publications()


if __name__ == '__main__':
    run()
