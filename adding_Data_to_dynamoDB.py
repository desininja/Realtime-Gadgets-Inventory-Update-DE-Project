import boto3
import csv

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table_name = 'inventory_data'
table = dynamodb.Table(table_name)

# Read data from CSV file and put items into DynamoDB table
with open('inventory_data.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Convert quantity to integer
        row['quantity'] = int(row['quantity'])
        
        # Put item into DynamoDB table
        table.put_item(Item=row)

print("Data loaded into DynamoDB table successfully.")
