import requests
import json
import boto3
import os
import uuid
from id_token import get_id_token

STAGE = 'sandbox'

def connect_author_to_feide(connect_author, connect_scn, connect_payload, idToken, scn, payload):
    if connect_author:
        connect_response = requests.post(
            'https://api.{}.nva.aws.unit.no/person/{}/identifiers/feideid/add'
            .format(STAGE, scn),
            json=payload)
        if not connect_response:
            print('POST /person/ {}'.format(connect_response.status_code))
    if not connect_author:
        token = 'Bearer ' + idToken
        delete_response = requests.delete(
            'https://api.{}.nva.aws.unit.no/person/{}/identifiers/feideid/delete'
            .format(STAGE, scn),
            json=payload,
            headers={'Authorization': token})
        if not delete_response:
            print('DELETE /person/{}/identifiers/feideid/delete {}'.format(
                scn, delete_response.status_code))


USER_POOL_ID = os.environ['AWS_USER_POOL_ID']
if not USER_POOL_ID:
    quit('Set environment variable AWS_USER_POOL_ID to correct User Pool Id')

CLIENT_ID = os.environ['AWS_USER_POOL_WEB_CLIENT_ID']
if not CLIENT_ID:
    quit('Set environment variable AWS_CLIENT_ID to correct Client Id')

person_query = 'https://api.{}.nva.aws.unit.no/person/?name={} {}'

test_users_file_name = './users/test_users.json'

client = boto3.client('cognito-idp')

def run():
    print('authors...')
    with open(test_users_file_name) as test_users_file:

        test_users = json.load(test_users_file)
        for test_user in test_users:
            givenName = test_user['givenName']
            familyName = test_user['familyName']
            connect_author = test_user['author']
            username = test_user['username']
            payload = {'identifier': username}

            idToken = get_id_token(username, client)

            query_response = requests.get(person_query.format(STAGE, givenName, familyName))
            if query_response.status_code != 200:
                print('GET /person/ {}'.format(query_response.status_code))
            if query_response.json() == []:
                inverted_name = '{}, {}'.format(familyName,givenName)
                new_author = {'invertedname': inverted_name}
                token = 'Bearer ' + idToken
                create_response = requests.post(
                    'https://api.{}.nva.aws.unit.no/person/'
                    .format(STAGE),
                    json=new_author,
                    headers={'Authorization': token})
                if not create_response:
                    print('POST /person/ {}'.format(create_response.status_code))
                else:
                    scn = create_response['systemControlNumber']
                    connect_author_to_feide(connect_author=connect_author,
                                            connect_scn=scn,
                                            connect_payload=payload,
                                            idToken=idToken,
                                            scn=scn,
                                            payload=payload)
            else:
                for item in query_response.json():
                    scn = item['systemControlNumber']
                    connect_author_to_feide(connect_author=connect_author,
                                            connect_scn=scn,
                                            connect_payload=payload,
                                            idToken=idToken,
                                            scn=scn,
                                            payload=payload)

run()