import boto3
import json
import copy
import uuid

client = boto3.client('dynamodb')
customer_template_file_name = './customers/institution.json'
test_customers_file_name = './customers/test_institutions.json'
customer_tablename = 'nva_customers'


def scan_customers():
    response = client.scan(TableName=customer_tablename)

    return response['Items']


def delete_customers():
    customers = scan_customers()
    for customer in customers:
        print(customer['identifier']['S'])
        identifier = customer['identifier']['S']
        if identifier.startswith('test_'):
            response = client.delete_item(
                TableName=customer_tablename,
                Key={'identifier': {
                    'S': identifier
                }})
            print(response)
    return


def put_item(new_customer):

    response = client.put_item(TableName=customer_tablename, Item=new_customer)
    return response


delete_customers()

with open(customer_template_file_name) as customer_template_file:
    customer_template = json.load(customer_template_file)

    with open(test_customers_file_name) as test_customers_file:

        test_customers = json.load(test_customers_file)
        for test_customer in test_customers:
            print(test_customer)
            new_customer = copy.deepcopy(customer_template)
            new_customer['administrationId']['S'] = test_customer[
                'administration_id']
            new_customer['archiveName']['S'] = test_customer['archive_name']
            new_customer['feideOrganizationId']['S'] = test_customer[
                'feide_orgnization_id']
            new_customer['identifier']['S'] = 'test_{}'.format(
                str(uuid.uuid4()))

            result = put_item(new_customer)
