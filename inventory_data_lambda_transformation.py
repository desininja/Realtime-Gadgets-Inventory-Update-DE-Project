import json
import boto3


dynamodb = boto3.resource('dynamodb')
table_name = 'inventory_data'
table = dynamodb.Table(table_name)



def lambda_handler(event, context):
    data = event[0]['data']
    print("Event Data: \n",data)
    
    try:
        if data['event_type']=='product_added':
            new_data = {'product_id':data['product']['product_id'],
                        'product_name':data['product']['product_name'],
                        'quantity':data['product']['quantity']
            }
        
            response = table.put_item(Item=new_data)
            print(f"Response after Adding a new item: {response}")
    
        elif data['event_type']=='product_removed':
            response = table.delete_item(Key={"product_id": data['product']['product_id']})
            print(f"Response after removing an item: {response}")
        
        elif data['event_type']=='product_quantity_increased':
            response = table.update_item(
                Key={"product_id": data['product']['product_id']},
                UpdateExpression="set quantity = quantity + :n",
                ExpressionAttributeValues={
                ":n": data['product']['quantity'],
                    },
                ReturnValues="UPDATED_NEW",
                )
            print(f"Response after increasing a product quantity: {response["Attributes"]}")
    
        else:
            response = table.update_item(
                Key={"product_id": data['product']['product_id']},
                UpdateExpression="set quantity = quantity - :n",
                ExpressionAttributeValues={
                ":n": data['product']['quantity'],
                    },
                ReturnValues="UPDATED_NEW",
                )
            print(f"Response after Decreasing a product quantity: {response["Attributes"]}")
        
        
    except Exception as e:
        print(e)

    return {
        'statusCode': 200,
        'body': json.dumps('DynamoDB data processed!')
    }
