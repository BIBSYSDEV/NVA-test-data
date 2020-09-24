import boto3
import uuid
import os

USER_POOL_ID = os.environ['AWS_USER_POOL_ID']
CLIENT_ID = os.environ['AWS_USER_POOL_WEB_CLIENT_ID']

def get_id_token(username, client):
    if not USER_POOL_ID:
        quit('Set environment variable AWS_USER_POOL_ID to correct User Pool Id')

    if not CLIENT_ID:
        quit('Set environment variable AWS_CLIENT_ID to correct Client Id')

    password = 'P%-' + str(uuid.uuid4())
    response = client.admin_set_user_password(
        Password=password,
        UserPoolId=USER_POOL_ID,
        Username=username,
        Permanent=True,
    )
    response = client.admin_initiate_auth(UserPoolId=USER_POOL_ID,
                                          ClientId=CLIENT_ID,
                                          AuthFlow='ADMIN_USER_PASSWORD_AUTH',
                                          AuthParameters={
                                              'USERNAME': username,
                                              'PASSWORD': password
                                          })
    return response['AuthenticationResult']['IdToken']
