import boto3
import json
import copy

client = boto3.client('dynamodb')


def scan_customers():
    response = client.scan(TableName='nva_customers')

    for item in response['Items']:
        print(item['identifier']['S'])
    return response['Items']


def delete_item(identifier):
    response = client.delete_item(TableName='nva_customers',
                                  Key={'identifier': {
                                      'S': identifier
                                  }})
    print(response)
    return response


def put_item(administrationId, archiveName, feideOrgnizationId, identifier):

    with open(customer_template_file) as customer:
        customer_template = json.load(customer)
        new_customer = copy.deepcopy(customer_template)
        new_customer['administrationId']['S'] = administrationId
        new_customer['archiveName']['S'] = archiveName
        new_customer['feideOrgnizationId']['S'] = feideOrgnizationId
        new_customer['identifier']['S'] = identifier

        response.put_item(TableName='nva_customers', Item=new_customer)
