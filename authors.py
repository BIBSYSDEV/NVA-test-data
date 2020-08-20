import requests
import json
import boto3
import os
import uuid


def create_user_map(client):
    cognito_users = client.list_users(UserPoolId=USER_POOL_ID)
    user_map = {}
    for user in cognito_users['Users']:
        cognito_username = user['Username']
        user_email = ''
        for attribute in user['Attributes']:
            if attribute['Name'] == 'email':
                user_email = attribute['Value']
        for attribute in user['Attributes']:
            if attribute['Name'] == 'custom:orgLegalName' and attribute[
                    'Value'] == 'TestOrg':
                user_map[user_email] = cognito_username
    return user_map


def get_id_token(username, client):
    password = 'P%-' + str(uuid.uuid4())
    response = client.admin_set_user_password(
        Password=password,
        UserPoolId=USER_POOL_ID,
        Username=user_map[username],
        Permanent=True,
    )
    response = client.admin_initiate_auth(UserPoolId=USER_POOL_ID,
                                          ClientId=CLIENT_ID,
                                          AuthFlow='ADMIN_USER_PASSWORD_AUTH',
                                          AuthParameters={
                                              'USERNAME': user_map[username],
                                              'PASSWORD': password,
                                          })
    return response['AuthenticationResult']['IdToken']


def connect_author_to_feide(connect_author, connect_scn, connect_payload):
    if connect_author:
        connect_response = requests.post(
            'https://api.sandbox.nva.aws.unit.no/person/{}/identifiers/feideid/add'
            .format(scn),
            json=payload)
        if not connect_response:
            print('POST /person/ {}'.format(response.status_code))
    if not connect_author:
        token = 'Bearer ' + idToken
        delete_response = requests.delete(
            'https://api.sandbox.nva.aws.unit.no/person/{}/identifiers/feideid/delete'
            .format(scn),
            json=payload,
            headers={'Authorization': token})
        if not delete_response:
            print('DELETE /person/{}/identifiers/feideid/delete {}'.format(
                scn, response.status_code))


USER_POOL_ID = os.environ['AWS_USER_POOL_ID']
if not USER_POOL_ID:
    quit('Set environment variable AWS_USER_POOL_ID to correct User Pool Id')

CLIENT_ID = os.environ['AWS_CLIENT_ID']
if not USER_POOL_ID:
    quit('Set environment variable AWS_CLIENT_ID to correct Client Id')

person_query = 'https://api.sandbox.nva.aws.unit.no/person/?name={}'

test_users_file_name = './users/test_users.json'

client = boto3.client('cognito-idp')

user_map = create_user_map(client)

with open(test_users_file_name) as test_users_file:

    test_users = json.load(test_users_file)
    for test_user in test_users:
        name = test_user['name']
        connect_author = test_user['author']
        username = test_user['username']
        payload = {'identifier': username}

        idToken = get_id_token(username, client)

        query_response = requests.get(person_query.format(name))
        if query_response.status_code != 200:
            print('GET /person/ {}'.format(resp.status_code))
        if query_response.json() == []:
            inverted_name = '{}, {}'.format(
                name.split(' ')[1],
                name.split(' ')[0])
            new_author = {'invertedname': inverted_name}
            token = 'Bearer ' + idToken
            create_response = requests.post(
                'https://api.sandbox.nva.aws.unit.no/person/',
                json=new_author,
                headers={'Authorization': token})
            if not create_response:
                print('POST /person/ {}'.format(create_response.status_code))
            else:
                scn = create_response['systemControlNumber']
                connect_author_to_feide(connect_author=connect_author,
                                        connect_scn=scn,
                                        connect_payload=payload)
        else:
            for item in query_response.json():
                scn = item['systemControlNumber']
                connect_author_to_feide(connect_author=connect_author,
                                        connect_scn=scn,
                                        connect_payload=payload)
