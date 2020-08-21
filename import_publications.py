import boto3
import json
import copy
import uuid

client = boto3.client('dynamodb')
publication_template_file_name = './publications/publication.json'
test_publications_file_name = './publications/test_publications.json'
publications_tablename = 'nva_publications'


def scan_publications():
    response = client.scan(TableName=publications_tablename)

    return response['Items']


def delete_publications():
    publications = scan_publications()
    for publication in publications:
        identifier = publication['identifier']['S']
        if identifier.startswith('test_'):
            response = client.delete_item(
                TableName=publication_tablename,
                Key={'identifier': {
                    'S': identifier
                }})
    return


def put_item(new_publication):

    response = client.put_item(TableName=publication_tablename,
                               Item=new_publication)
    return response


delete_publications()

with open(publication_template_file_name) as publication_template_file:
    publication_template = json.load(publication_template_file)

    with open(test_publications_file_name) as test_publications_file:

        test_publications = json.load(test_publications_file)
        for test_publication in test_publications:
            new_publication = copy.deepcopy(publication_template)
            new_publication['administrationId']['S'] = test_publication[
                'administration_id']
            new_publication['archiveName']['S'] = test_publication[
                'archive_name']
            new_publication['feideOrganizationId']['S'] = test_publication[
                'feide_orgnization_id']
            new_publication['identifier']['S'] = 'test_{}'.format(
                str(uuid.uuid4()))

            result = put_item(new_publication)
