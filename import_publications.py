import boto3
import json
import copy
import uuid
import requests
import uuid
from os import listdir

dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')
resource_template_file_name = './publications/resource.json'
publication_template_file_name = './publications/publication.json'
test_publications_file_name = './publications/test_publications.json'
# publications_tablename = 'nva_resources'
publications_tablename = 'nva-resources-nva-publication-api-nva-publication-api'
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
    print(len(resources))
    for resource in resources:
        if resource['type']['S'] == 'Resource':
            publication = resource['data']['M']
            primary_partition_key = resource['PK0']['S']
            primary_sort_key = resource['SK0']['S']
            identifier = publication['identifier']['S']
            owner = publication['owner']['S']
            if 'test.no' in owner:
                print(
                    'Deleting {} - {}'.format(publication['identifier']['S'], owner))
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


def put_item(new_publication):

    try:
        response = dynamodb_client.put_item(TableName=publications_tablename,
                                            Item=new_publication)
    except:
        print(sys.exc_info()[0])
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

def create_contributor(contributor):
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

        return new_contributor

def create_pk0(pk0_template, customer, username):
    pk0 = pk0_template.replace('<type>', 'Resource').replace('<customerId>', customer).replace('<userId>', username)
    return pk0

def create_pk1(pk1_template, customer, status):
    pk1 = pk1_template.replace('<type>', 'Resource').replace('<customerId>', customer).replace('<status>', status)
    return pk1

def create_pk2(pk2_template, customer, identifier):
    pk2 = pk2_template.replace('<type>', 'Resource').replace('<customerId>', customer).replace('<resourceId>', identifier)
    return pk2

def create_resource_key(template, identifier):
    key = template.replace('<type>', 'Resource').replace('<resourceId>', identifier)
    return key

def create_resource(resource_template, customer, identifier, username, status):
    new_resource = copy.deepcopy(resource_template)
    new_resource['PK0']['S'] = create_pk0(pk0_template=str(new_resource['PK0']['S']), customer=customer, username=username)
    new_resource['PK1']['S'] = create_pk1(pk1_template=str(new_resource['PK1']['S']), customer=customer, status=status)
    new_resource['PK2']['S'] = create_pk2(pk2_template=str(new_resource['PK2']['S']), customer=customer, identifier=identifier)
    new_resource['PK3']['S'] = create_resource_key(template=str(new_resource['PK3']['S']), identifier=identifier)
    new_resource['SK0']['S'] = create_resource_key(template=str(new_resource['SK0']['S']), identifier=identifier)
    new_resource['SK1']['S'] = create_resource_key(template=str(new_resource['SK1']['S']), identifier=identifier)
    new_resource['SK2']['S'] = create_resource_key(template=str(new_resource['SK2']['S']), identifier=identifier).replace('<resourceSort>', 'a')
    new_resource['SK3']['S'] = create_resource_key(template=str(new_resource['SK3']['S']), identifier=identifier)
    new_resource['type']['S'] = 'Resource'
    return new_resource

def create_publication_data(publication_template, test_publication, identifier, username, customer, status):
    new_publication = copy.deepcopy(publication_template)
    new_publication['identifier']['S'] = identifier
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
    new_publication['owner']['S'] = username
    new_publication['publisher']['M']['id']['S'] = customer
    new_publication['status']['S'] = status

    if test_publication['contributor'] != '':
        contributor = test_publication['contributor']
        new_contributor = create_contributor(contributor=contributor)
        new_publication['entityDescription']['M']['contributors'][
            'L'].append(new_contributor)

    return new_publication

def create_test_publication(publication_template, resource_template, test_publication):
    customer = get_customer(test_publication['owner']).replace('https://api.dev.nva.aws.unit.no/customer/', '')
    identifier = str(uuid.uuid4())
    username = test_publication['owner']
    status = test_publication['status']

    new_resource = create_resource(
        resource_template=resource_template, 
        customer=customer, 
        identifier=identifier, 
        username=username, 
        status=status
    )

    new_publication = create_publication_data(
        publication_template=publication_template, 
        test_publication=test_publication, 
        identifier=identifier, 
        username=username, 
        customer=customer, 
        status=status
    )

    new_resource['data']['M'] = new_publication

    return new_resource

def create_publications():
    with open(publication_template_file_name) as publication_template_file:
        publication_template = json.load(publication_template_file)

    with open(resource_template_file_name) as resource_template_file:
        resource_template = json.load(resource_template_file)

    with open(test_publications_file_name) as test_publications_file:

        test_publications = json.load(test_publications_file)
        for test_publication in test_publications:

            new_publication = create_test_publication(
                publication_template=publication_template,
                resource_template=resource_template,
                test_publication=test_publication
            )
            print(test_publication['title'])
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
