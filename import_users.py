import boto3
import json
import os
import copy
import requests

ROLE_TABLENAME = 'UsersAndRolesTable'
CUSTOMER_TABLENAME = 'nva_customers'

ssm = boto3.client('ssm')
USER_POOL_ID = ssm.get_parameter(Name='/test/AWS_USER_POOL_ID',
                                 WithDecryption=False)['Parameter']['Value']
CLIENT_ID = ssm.get_parameter(Name='/test/AWS_USER_POOL_WEB_CLIENT_ID',
                              WithDecryption=False)['Parameter']['Value']
STAGE = ssm.get_parameter(Name='/test/STAGE',
                          WithDecryption=False)['Parameter']['Value']
CUSTOMER_ID = ssm.get_parameter(Name='/test/TEST_CUSTOMER',
                                WithDecryption=False)['Parameter']['Value']

ROLE_TEMPLATE_FILE_NAME = './users/role.json'
DB_CLIENT = boto3.client('dynamodb')


def findCustomer(org_number):
    try:
        response = DB_CLIENT.query(
            ExpressionAttributeValues={':v1': {
                'S': org_number
            }},
            KeyConditionExpression="feideOrganizationId = :v1",
            ProjectionExpression="identifier",
            TableName=CUSTOMER_TABLENAME,
            IndexName='byOrgNumber')
        print(response['Items'][0]['identifier']['S'])
        return response['Items'][0]['identifier']['S']
    except:
        print('Customer not found: {}'.format(org_number))
        pass


def createRole(test_user):

    with open(ROLE_TEMPLATE_FILE_NAME) as role_template_file:
        role_template = json.load(role_template_file)

        given_name = test_user['givenName']
        family_name = test_user['familyName']
        username = test_user['username']
        role = test_user['role']
        org_number = test_user['orgNumber']
        customer_iri = 'https://api.{}.nva.aws.unit.no/customer/{}'.format(
            STAGE, findCustomer(org_number))

        customer_identifier = findCustomer(test_user['orgNumber'])

        new_role = copy.deepcopy(role_template)
        new_role['familyName']['S'] = family_name
        new_role['givenName']['S'] = given_name
        new_role['institution']['S'] = customer_iri
        new_role['PrimaryKeyHashKey']['S'] = 'USER#{}'.format(username)
        new_role['PrimaryKeyRangeKey']['S'] = 'USER#{}'.format(username)
        new_role['SecondaryIndex1HashKey']['S'] = customer_iri
        new_role['SecondaryIndex1RangeKey']['S'] = username
        new_role['roles']['L'][0]['M']['name']['S'] = role
        new_role['roles']['L'][0]['M']['PrimaryKeyHashKey'][
            'S'] = 'ROLE#{}'.format(role)
        new_role['username']['S'] = username

        response = DB_CLIENT.put_item(TableName=ROLE_TABLENAME, Item=new_role)


def deleteRole(username):
    response = DB_CLIENT.delete_item(TableName=ROLE_TABLENAME,
                                     Key={
                                         'PrimaryKeyHashKey': {
                                             'S': 'USER#{}'.format(username)
                                         },
                                         'PrimaryKeyRangeKey': {
                                             'S': 'USER'
                                         }
                                     })
    return


def run():
    print('users...')
    if not USER_POOL_ID:
        quit(
            'Set environment variable AWS_USER_POOL_ID to correct User Pool Id'
        )

    user_attribute_file_name = './users/user.json'
    test_users_file_name = './users/test_users.json'

    with open(user_attribute_file_name) as json_file:
        user = json.load(json_file)

        COGNITO_CLIENT = boto3.client('cognito-idp')
        cognito_test_users = []
        response = COGNITO_CLIENT.list_users(UserPoolId=USER_POOL_ID)
        for cognito_user in response['Users']:
            for attribute in cognito_user['Attributes']:
                if attribute['Name'] == 'custom:orgLegalName' and attribute[
                        'Value'] == 'TestOrg':
                    cognito_test_users.append(cognito_user['Username'])
        for cognito_test_username in cognito_test_users:
            try:
                response = COGNITO_CLIENT.admin_delete_user(
                    UserPoolId=USER_POOL_ID, Username=cognito_test_username)
            except:
                print('Error deleting users')
                pass

        with open(test_users_file_name) as test_users_file:

            test_users = json.load(test_users_file)
            for test_user in test_users:

                family_name = test_user['familyName']
                given_name = test_user['givenName']
                username = test_user['username']
                org_number = test_user['orgNumber']
                affiliation = test_user['affiliation']
                cristinId = test_user['cristinId']
                user_attributes = copy.deepcopy(user)

                for attribute in user_attributes:
                    if attribute['Name'] == 'custom:identifiers':
                        attribute['Value'] = 'feide:{}'.format(username)
                    if attribute['Name'] == 'custom:feideId' or attribute[
                            'Name'] == 'email':
                        attribute['Value'] = username
                    if attribute['Name'] == 'name' or attribute[
                            'Name'] == 'custom:commonName':
                        attribute['Value'] = '{} {}'.format(
                            given_name, family_name)
                    if attribute['Name'] == 'custom:orgNumber':
                        attribute['Value'] = org_number
                    if attribute['Name'] == 'custom:cristinId':
                        attribute['Value'] = cristinId
                    if attribute['Name'] == 'custom:affiliation':
                        attribute['Value'] = 'feide:{}'.format(affiliation)
                    if attribute['Name'] == 'custom:customerId':
                        attribute['Value'] = attribute['Value'].format(
                            STAGE, CUSTOMER_ID)

                try:
                    response = COGNITO_CLIENT.admin_create_user(
                        UserPoolId=USER_POOL_ID,
                        Username=username,
                        UserAttributes=user_attributes,
                        MessageAction='SUPPRESS')
                except:
                    print('Error creating users')
                    pass

                role = test_user['role']
                deleteRole(username)
                createRole(test_user)


if __name__ == '__main__':
    run()
