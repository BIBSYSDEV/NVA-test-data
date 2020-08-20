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


def put_item(administration_id, archive_name, feide_orgnization_id,
             identifier):

    with open(customer_template_file) as customer:
        customer_template = json.load(customer)
        new_customer = copy.deepcopy(customer_template)
        new_customer['administrationId']['S'] = administration_id
        new_customer['archiveName']['S'] = archive_name
        new_customer['feideOrgnizationId']['S'] = feide_orgnization_id
        new_customer['identifier']['S'] = identifier

        response = client.put_item(TableName='nva_customers',
                                   Item=new_customer)
        return response


test_administation_id = 'test@test.no'
test_archive_name = 'test archive'
test_feide_org_id = '0987654321'
test_identifier = ''