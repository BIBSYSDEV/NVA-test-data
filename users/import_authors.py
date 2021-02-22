import requests
import json
import boto3
import os
import uuid
from id_token import get_id_token
import users.clear_arp

ssm = boto3.client('ssm')
USER_POOL_ID = ssm.get_parameter(Name='/test/AWS_USER_POOL_ID',
                                 WithDecryption=False)['Parameter']['Value']
CLIENT_ID = ssm.get_parameter(Name='/test/AWS_USER_POOL_WEB_CLIENT_ID',
                              WithDecryption=False)['Parameter']['Value']
STAGE = ssm.get_parameter(Name='/test/STAGE',
                          WithDecryption=False)['Parameter']['Value']
CUSTOMER_ID = ssm.get_parameter(Name='/test/TEST_CUSTOMER',
                                WithDecryption=False)['Parameter']['Value']

person_query = 'https://api.{}.nva.aws.unit.no/person/?feideid={}'

test_users_file_name = './users/test_users.json'

client = boto3.client('cognito-idp')


def connect_author(id_token, id,
                   payload, connection_type):

    if connect_author:
        token = 'Bearer ' + id_token
        connect_response = requests.post(
            'https://api.{}.nva.aws.unit.no/person/{}/identifiers/{}/add'.
            format(STAGE, id, connection_type),
            json=payload,
            headers={'Authorization': token})
        if not connect_response:
            print('POST /person/ {} {}'.format(connect_response.status_code,
                                               connect_response.reason))
    if not connect_author:
        token = 'Bearer ' + id_token
        delete_response = requests.delete(
            'https://api.{}.nva.aws.unit.no/person/{}/identifiers/{}/delete'
            .format(STAGE, id, connection_type),
            json=payload,
            headers={'Authorization': token})
        if not delete_response:
            print('DELETE /person/{}/identifiers/{}/delete {}'.format(
                scn, connectionType, delete_response.status_code))


def create_author(family_name, given_name, id_token, has_author, payload):
    inverted_name = '{}, {}'.format(family_name, given_name)
    new_author = {'invertedname': inverted_name}
    token = 'Bearer ' + id_token
    create_response = requests.post(
        'https://api.{}.nva.aws.unit.no/person/'.format(STAGE),
        json=new_author,
        headers={'Authorization': token})
    if not create_response:
        print('POST /person/ {}'.format(
            create_response.status_code))
    else:
        id = create_response.json()['id'].split('/')[-1]
        if has_author:
            connect_author(id_token=id_token,
                           id=id,
                           payload=payload,
                           connection_type='feideid')


def run():
    users.clear_arp.run()
    print('authors...')
    with open(test_users_file_name) as test_users_file:

        test_users = json.load(test_users_file)
        for test_user in test_users:
            given_name = test_user['givenName']
            family_name = test_user['familyName']
            has_author = test_user['author']
            username = test_user['username']
            feideid_payload = {'identifier': username}
            orcid = test_user['orcid']

            print(username)

            id_token = get_id_token(username, client)

            query_response = requests.get(
                person_query.format(STAGE, username))
            if query_response.status_code != 200:
                print('GET /person/ {}'.format(resp.status_code))
            if query_response.json() == []:
                create_author(family_name=family_name,
                              given_name=given_name, id_token=id_token, has_author=has_author, payload=feideid_payload)
            else:
                for item in query_response.json():
                    id = item['id'].split('/')[-1]
                    if has_author:
                        connection_type = 'feideid'
                        connect_author(id_token=id_token,
                                       id=id,
                                       payload=feideid_payload,
                                       connection_type=connection_type)
                    if orcid:
                        connection_type = 'orcid'
                        payload = {'identifier': username}
                        connect_author(id_token=id_token,
                                       id=id,
                                       payload=feideid_payload,
                                       connection_type=connection_type)


if __name__ == '__main__':
    run()
