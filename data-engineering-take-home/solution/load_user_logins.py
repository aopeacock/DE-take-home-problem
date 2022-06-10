import localstack_client.session as boto3
import json
import hashlib
import psycopg2
from datetime import datetime


# psycopg2 library is used to connect to the postgres database
# Would be better to store connection details in a configuration file included in .gitignore
conn = psycopg2.connect(
    "host=localhost dbname=postgres user=postgres password=postgres")

# boto3 client setup to connect to Amazon SQS
client = boto3.client('sqs')


def receiveMessage(client):
    '''
    receiveMessage() used to receive the message from the SQS queue.
    MaxNumberOfMessages set to 100 to receive every test record.
T   here is a better way to set this up rather than batching 100
    records at a time***
    '''
    response = client.receive_message(
        QueueUrl='http://localhost:4566/000000000000/login-queue',
        AttributeNames=[
            'Body'
        ],
        MessageAttributeNames=[
            'user_id',
            'app_version',
            'device_type',
            'ip',
            'locale',
            'device_id'
        ],
        MaxNumberOfMessages=100,
        VisibilityTimeout=100,
        WaitTimeSeconds=100,
        ReceiveRequestAttemptId='string'
    )
    return response


def flattenData(res):
    '''
    flattenData() used to create a list which contains a record in each index of the list.
    Each record in the list is stored as a dictionary where the column name
    is the key
    '''
    li = []
    # The Body in Messages contains each record in the response
    # received from SQS
    for record in res['Messages']:
        li.append(json.loads(record['Body']))
    return li


def transformations(li):
    '''
    transformations() used to mask the values for IP and DEVICE_ID with a hash.
    The hash will be unique based on the input value.
    sha256 hashing algo was used, but there may be a more effecient one.
    Values are hashed in place so nothing is returned.
    A create datetime value is also added to the end of each record.
    '''
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    for record in li:
        hash_ip = record['ip'].encode()
        hash_device_id = record['device_id'].encode()
        record['ip'] = hashlib.sha256(hash_ip).hexdigest()
        record['device_id'] = hashlib.sha256(hash_device_id).hexdigest()
        record['create_date'] = dt_string


def insertIntoTable(records, table_name):
    '''
    insertIntoTable() used to insert the records into the table.
    excutemany method will iterate through every dictionary in our list of records.
    '''
    cur = conn.cursor()
    cur.executemany("""INSERT INTO user_logins(user_id,device_type,masked_ip,masked_device_id,locale,app_version,create_date) VALUES (%(user_id)s, %(device_type)s, %(ip)s, %(device_id)s, %(locale)s, %(app_version)s, %(create_date)s)""", records)
    conn.commit()
    cur.close()


def main():
    '''
    main() used the program
    receiveMessage() is called passing in the client to receive the message from Amazon SQS
    A list is created with flattenData() passing the response. The returned list has a record in each
    index, each record is converted from JSON format to a dictionary.
    The transformations() function is called passing in the list to mask the 2 necessary fields and add a create_date column to each record
    Finally insertIntoTable() is called passing in the list of records for insertion into the user_logins table.
    '''
    print('starting execution')
    print('----------------------------')
    response = receiveMessage(client)
    li = flattenData(response)
    transformations(li)
    insertIntoTable(li, None)
    print('----------------------------')
    print('end of execution')


if __name__ == "__main__":
    main()
