import boto3
import uuid

ssm = boto3.client('ssm')
USER_POOL_ID = ssm.get_parameter(Name='/test/AWS_USER_POOL_ID',
                                 WithDecryption=False)['Parameter']['Value']
CLIENT_ID = ssm.get_parameter(Name='/test/AWS_USER_POOL_WEB_CLIENT_ID',
                              WithDecryption=False)['Parameter']['Value']
username = 'test-data-user@test.no'

def login():
    client = boto3.client('cognito-idp')
    password = 'P%' + str(uuid.uuid4())
    response = client.admin_set_user_password(
        Password=password,
        UserPoolId=USER_POOL_ID,
        Username=username,
        Permanent=True,
    )
    response = client.admin_initiate_auth(
        UserPoolId=USER_POOL_ID,
        ClientId=CLIENT_ID,
        AuthFlow='ADMIN_USER_PASSWORD_AUTH',
        AuthParameters={
            'USERNAME': username,
            'PASSWORD': password
        })
    return response['AuthenticationResult']['IdToken']
