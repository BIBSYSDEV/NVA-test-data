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
        identifier = customer['identifier']['S']
        if identifier.startswith('test_'):
            response = client.delete_item(
                TableName=customer_tablename,
                Key={'identifier': {
                    'S': identifier
                }})
    return

def create_customers():
    with open(customer_template_file_name) as customer_template_file:
        customer_template = json.load(customer_template_file)

        with open(test_customers_file_name) as test_customers_file:

            test_customers = json.load(test_customers_file)
            for test_customer in test_customers:
                new_customer = copy.deepcopy(customer_template)
                new_customer['feideOrganizationId']['S'] = test_customer[
                    'feide_organization_id']
                new_customer['identifier']['S'] = 'test_{}'.format(
                    str(uuid.uuid4()))

                result = put_item(new_customer)

def put_item(new_customer):

    response = client.put_item(
        TableName=customer_tablename, 
        Item=new_customer)
    return response

def run():
    print('customers...')
    delete_customers()
    create_customers()

run()