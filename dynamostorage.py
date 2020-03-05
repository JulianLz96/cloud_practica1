import boto3
import keyvalue.parsetriples as ParseTripe
import boto.dynamodb2
from boto.dynamodb2.table import Table
import keyvalue.stemmer as Stemmer
import sys

dynamodb = boto3.resource('dynamodb')
dynamodbClient = boto3.client('dynamodb')

def createImagesTable(name):
    table = dynamodb.create_table(
            TableName=name,
            KeySchema=[
                {
                    'AttributeName': 'key',
                    'KeyType': 'HASH'  #Partition key
                },
                {
                    'AttributeName': 'sort',
                    'KeyType': 'RANGE'  #Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'key',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'sort',
                    'AttributeType': 'S'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
    table.meta.client.get_waiter('table_exists').wait(TableName=name)
    print("Table created")
    return table

def storage(num_images):
    try: 
        images = dynamodbClient.describe_table(TableName="images")
    except dynamodbClient.exceptions.ResourceNotFoundException:
        images = createImagesTable('images')

    try: 
        terms = dynamodbClient.describe_table(TableName="labels")
    except dynamodbClient.exceptions.ResourceNotFoundException:
        images = createImagesTable('labels')

    #Parse .ttl
    images_ttl = ParseTripe.ParseTriples("resources/images.ttl") 
    labels_ttl = ParseTripe.ParseTriples("resources/labels_en.ttl")

    # images_arr={}
    temp_category={}
    only_categ=[]
    while_images = int(num_images)

    #While para tabla imagenes
    while(while_images > 0):
        temp_images = images_ttl.getNext()
        if(temp_images[1] == "http://xmlns.com/foaf/0.1/depiction"):
            # define put(str(temp_images[0]),str(temp_images[2]))
            categoria = temp_images[0]
            img = temp_images[2]
            # if images_arr.get(categoria) is None:
            #     images_arr[categoria] = 1
            # else:
            #     images_arr[categoria] += 1
            if not temp_images[0] in only_categ:
                only_categ.append(temp_images[0])
            put('images',categoria,categoria.split("/")[4], img)
            print(categoria)
            #put('images',categoria,str(images_arr[categoria]), img)
        while_images -= 1

    while_images = int(num_images)

    #Upload labels
    while while_images > 0:
        temp_labels = labels_ttl.getNext()
        if(temp_labels[1] == "http://www.w3.org/2000/01/rdf-schema#label"):
            len_labels = temp_labels[2].split(" ")
            sort_value=temp_labels[0].split("/")[4]
            value=temp_labels[0]
            if(len(len_labels) > 1):
                for label in len_labels:
                    if temp_labels[0] in only_categ:
                        stemmed_word=Stemmer.stem(label)
                        put('labels',stemmed_word, sort_value,value)
            else:
                put('labels',stemmed_word, sort_value,value)
        while_images-=1
    
    print("Loaded images")

def main(argv):
    sys.argv.pop(0)
    storage(sys.argv[0])

#table es el nombre
def put(table, key, sort, value):
    res = dynamodbClient.put_item(
        TableName = table,
        Item = {
            'key': {
                'S':key
            },
            'sort': {
                'S':sort
            },
            'value': {
                'S':value
            }
        }
    )
    return res

#table es el objeto pero no funciona gg
def put2(table, key, sort, value):
    table2=Table(table)
    print(table2)
    with table2.batch_write() as batch:
        result = batch.put_item(Item={
            'partition_key': key,
            'sort_key': sort,
            'value': value
        })

if __name__ == "__main__":
    main(sys.argv)