import requests
import json
import boto3
import os
import uuid
from id_token import get_id_token

ssm = boto3.client('ssm')
USER_POOL_ID = ssm.get_parameter(Name='/test/AWS_USER_POOL_ID',
                                 WithDecryption=False)['Parameter']['Value']
CLIENT_ID = ssm.get_parameter(Name='/test/AWS_USER_POOL_WEB_CLIENT_ID',
                              WithDecryption=False)['Parameter']['Value']
STAGE = ssm.get_parameter(Name='/test/STAGE',
                          WithDecryption=False)['Parameter']['Value']

person_query = 'https://api.{}.nva.aws.unit.no/person/?name={} {}'

test_users_file_name = './users/test_users.json'

client = boto3.client('cognito-idp')


def connect_author_to_feide(connect_author, connect_scn, idToken, scn,
                            feideid_payload):
    if connect_author:
        connect_response = requests.post(
            'https://api.{}.nva.aws.unit.no/person/{}/identifiers/feideid/add'.
            format(STAGE, scn),
            json=feideid_payload)
        if not connect_response:
            print('POST /person/ {}'.format(connect_response.status_code))
        if not connect_response:
            print('POST /person/ {}'.format(connect_response.status_code))
    if not connect_author:
        token = 'Bearer ' + idToken
        delete_response = requests.delete(
            'https://api.{}.nva.aws.unit.no/person/{}/identifiers/feideid/delete'
            .format(STAGE, scn),
            json=feideid_payload,
            headers={'Authorization': token})
        if not delete_response:
            print('DELETE /person/{}/identifiers/feideid/delete {}'.format(
                scn, delete_response.status_code))


def run():
    print('authors...')
    with open(test_users_file_name) as test_users_file:

        test_users = json.load(test_users_file)
        for test_user in test_users:
            givenName = test_user['givenName']
            familyName = test_user['familyName']
            connect_author = test_user['author']
            username = test_user['username']
            feideid_payload = {'identifier': username}

            idToken = get_id_token(username, client)

            query_response = requests.get(
                person_query.format(STAGE, givenName, familyName))
            if query_response.status_code != 200:
                print('GET /person/ {}'.format(resp.status_code))
            if query_response.json() == []:
                inverted_name = '{}, {}'.format(familyName, givenName)
                new_author = {'invertedname': inverted_name}
                token = 'Bearer ' + idToken
                create_response = requests.post(
                    'https://api.{}.nva.aws.unit.no/person/'.format(STAGE),
                    json=new_author,
                    headers={'Authorization': token})
                if not create_response:
                    print('POST /person/ {}'.format(
                        create_response.status_code))
                else:
                    scn = create_response['systemControlNumber']
                    connect_author_to_feide(connect_author=connect_author,
                                            connect_scn=scn,
                                            idToken=idToken,
                                            scn=scn,
                                            feideid_payload=feideid_payload)
            else:
                for item in query_response.json():
                    scn = item['systemControlNumber']
                    connect_author_to_feide(connect_author=connect_author,
                                            connect_scn=scn,
                                            idToken=idToken,
                                            scn=scn,
                                            feideid_payload=feideid_payload)


if __name__ == '__main__':
    run()