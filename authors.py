import requests
import json

person_query = 'https://api.sandbox.nva.aws.unit.no/person/?name={}'

test_users_file_name = './users/test_users.json'

with open(test_users_file_name) as test_users_file:

    test_users = json.load(test_users_file)
    for test_user in test_users:
        name = test_user['name']
        print(name)
        response = requests.get(person_query.format(name))
        if response.status_code != 200:
            raise ApiError('GET /person/ {}'.format(resp.status_code))
        if response.json() == []:
            print('{} not found in ARP'.format(name))
        for item in response.json():
            print(item)
