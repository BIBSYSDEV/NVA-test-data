import boto3
import json
import os
import copy

def run():
    print('users...')
    USER_POOL_ID = os.environ['AWS_USER_POOLS_ID']
    if not USER_POOL_ID:
        quit('Set environment variable AWS_USER_POOL_ID to correct User Pool Id')

    user_attribute_file_name = './users/user.json'
    test_users_file_name = './users/test_users.json'

    with open(user_attribute_file_name) as json_file:
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
            try:
                response = client.admin_delete_user(UserPoolId=USER_POOL_ID,
                                                    Username=cognito_test_username)
            except:
                print('Error deleting users')
                pass

        with open(test_users_file_name) as test_users_file:

            test_users = json.load(test_users_file)
            for test_user in test_users:
                name = test_user['name']
                username = test_user['username']
                user_attributes = copy.deepcopy(user)

                for attribute in user_attributes:
                    if attribute['Name'] == 'custom:identifiers':
                        attribute['Value'] = 'feide:{}'.format(username)
                    if attribute['Name'] == 'custom:feideId' or attribute[
                            'Name'] == 'email':
                        attribute['Value'] = username
                    if attribute['Name'] == 'name' or attribute[
                            'Name'] == 'custom:commonName':
                        attribute['Value'] = name

                try:
                    response = client.admin_create_user(
                        UserPoolId=USER_POOL_ID,
                        Username=username,
                        UserAttributes=user_attributes,
                        MessageAction='SUPPRESS')
                except:
                    print('Error creating users')
                    pass