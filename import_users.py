import boto3
import json
import os
import copy

USER_POOL_ID = os.environ['AWS_USER_POOL_ID']
if not USER_POOL_ID:
    quit('Set AWS_USER_POOL_ID to correct User Pool Id')

with open('./users/user.json') as json_file:
    user = json.load(json_file)

    client = boto3.client('cognito-idp')
    cognito_test_users = []
    response = client.list_users(UserPoolId=USER_POOL_ID)
    for cognito_user in response['Users']:
        for attribute in cognito_user['Attributes']:
            if attribute['Name'] == 'custom:orgLegalName' and attribute[
                    'Value'] == 'TestOrg':
                cognito_test_users.append(cognito_user['Username'])
    for cognito_test_username in cognito_test_users:
        response = client.admin_delete_user(UserPoolId=USER_POOL_ID,
                                            Username=cognito_test_username)

    with open('./users/test_users.json') as test_users_file:

        test_users = json.load(test_users_file)
        for test_user in test_users:
            name = test_user['name']
            username = test_user['username']
            user_attributes = copy.deepcopy(user)

            for attribute in user_attributes:
                if attribute['Name'] == 'custom:identifiers':
                    attribute['Value'] = 'feide:%s' % username
                if attribute['Name'] == 'custom:feideId' or attribute[
                        'Name'] == 'email':
                    attribute['Value'] = username
                if attribute['Name'] == 'name' or attribute[
                        'Name'] == 'custom:commonName':
                    attribute['Value'] = name

            response = client.admin_create_user(UserPoolId=USER_POOL_ID,
                                                Username=username,
                                                UserAttributes=user_attributes,
                                                MessageAction='SUPPRESS')
            print(response)
