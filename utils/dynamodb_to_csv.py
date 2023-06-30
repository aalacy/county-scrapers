import csv
import boto3
import json

TABLE_NAME = 'miamidadeclerk'
dynamo_dbs = ['miamidadeclerk', 'miamidadeclerk_2', 'miamidadeclerk_3']
TEMP_FILENAME = 'allpdf.csv'

dynamodb_resource = boto3.resource('dynamodb', region_name='us-east-2', aws_access_key_id='AKIAYJCUALZWFGZS6Z6S', aws_secret_access_key='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')


def get_csv_from_dynamodb():
    with open(TEMP_FILENAME, 'w') as output_file:
        writer = csv.writer(output_file)
        header = True
        first_page = True

        for tabl in dynamo_dbs:
            first_page = True
            print("start parse {}".format(tabl))
            table = dynamodb_resource.Table(tabl)
            # Paginate results
            while True:

                # Scan DynamoDB table
                if first_page:
                    response = table.scan()
                    first_page = False
                else:
                    response = table.scan(ExclusiveStartKey = response['LastEvaluatedKey'])

                for item in response['Items']:

                    # Write header row?
                    if header:
                        writer.writerow(item.keys())
                        header = False

                    writer.writerow(item.values())

                # Last page?
                if 'LastEvaluatedKey' not in response:
                    break

if __name__ == '__main__':
    get_csv_from_dynamodb()