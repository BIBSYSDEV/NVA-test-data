import requests
import json
import boto3
import os
from id_token import get_id_token
import copy

ssm = boto3.client('secretsmanager')

QUERY_URL = 'https://alfa-a.bibsys.no/authority/rest/functions/v2/query?q={}&start=1&max=500'
DELETE_URL = 'https://alfa-a.bibsys.no/authority/rest/authorities/v2/{}'
CREATE_URL = 'https://alfa-a.bibsys.no/authority/rest/authorities/v2'
BARE_API_KEY = ssm.get_secret_value(SecretId='bareApiKey')['SecretString']
BARE_HEADER = {'Authorization': 'apikey {}'.format(BARE_API_KEY)}


def delete_authors(query):
    response = requests.get(QUERY_URL.format(query))
    searchResult = response.json()
    for auth in searchResult['results']:
        print('deleting: {}'.format(auth['systemControlNumber']))
        delete_response = requests.delete(DELETE_URL.format(auth['systemControlNumber']),
                                          headers=BARE_HEADER)
        if 'feide' in auth['identifiersMap']:
            for feideId in auth['identifiersMap']['feide']:
                print('...{}'.format(feideId))

# create authors that should pre-exist in ARP


def create_authors():
    with open('./users/author.json') as author_template_file:
        author_template = json.load(author_template_file)
        with open('./users/test_authors.json') as authors_file:
            authors = json.load(authors_file)
            for author in authors:
                delete_authors(query=author['name'])
                name = {
                    'tag': '100',
                    'ind1': '1',
                    'ind2': '',
                    'subfields': [
                        {
                            'subcode': 'a',
                            'value': author['name']
                        }
                    ]
                }
                payload = copy.deepcopy(author_template)
                payload['marcdata'].append(name)
                response = requests.post(
                    CREATE_URL, json=payload, headers=BARE_HEADER)
                # print(response)


def run():
    delete_authors('TestUser')
    create_authors()


if __name__ == '__main__':
    run()
