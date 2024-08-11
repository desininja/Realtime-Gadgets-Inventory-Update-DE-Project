import boto3
import json
import random
import pandas as pd
from datetime import datetime
import time
import csv

def data_generator(inv_products_id,inv_products_name):

    event_type = random.choice(['product_added','product_removed','product_quantity_increased','product_quantity_decreased'])
    print(event_type)

    new_product_id = "P_"+str(random.randint(10,500))
    new_product_name = random.choice(["Smartphone","Laptop","Tablet","Smartwatch","Desktop computer","Digital camera","Television (Smart TV)",
                                            "Headphones","Bluetooth speaker","Gaming console","Fitness tracker","E-reader","Drone","Virtual reality headset",
                                            "Home assistant","GPS navigation device","Digital voice recorder","Portable power bank","Wireless router","Smart thermostat"])
    quantity = random.randint(10,100)

    if event_type =='product_added':
        
        while(new_product_id in inv_products_id and new_product_name in inv_products_name):
            new_product_id = "P_"+str(random.randint(10,500))
            new_product_name = random.choice(["Smartphone","Laptop","Tablet","Smartwatch","Desktop computer","Digital camera","Television (Smart TV)",
                                            "Headphones","Bluetooth speaker","Gaming console","Fitness tracker","E-reader","Drone","Virtual reality headset",
                                            "Home assistant","GPS navigation device","Digital voice recorder","Portable power bank","Wireless router","Smart thermostat"])

        csv_file_path = 'product_data.csv'
        with open(csv_file_path, mode='a', newline='') as file:
            writer =csv.writer(file)
            writer.writerow([new_product_id,new_product_name])

        return ({"event_type":event_type,
                 "product":{"product_id":new_product_id,
                            "product_name":new_product_name,
                            "quantity":quantity,
                            "timestamp":datetime.now()}})
    
    elif event_type =='product_removed':
        product_id = random.choice(inv_products_id)
        print(product_id)
        index = inv_products_id.index(product_id)
        product_name = inv_products_name[index]
        quantity = 0

        return ({"event_type":event_type,
                 "product":{"product_id":product_id,
                            "product_name":product_name,
                            "quantity":quantity,
                            "timestamp":datetime.now()}})
    
    elif event_type == 'product_quantity_increased':
        product_id = random.choice(inv_products_id)
        print(product_id)
        index = inv_products_id.index(product_id)
        product_name = inv_products_name[index]
        quantity = random.randint(10,20)

        return ({"event_type":event_type,
                 "product":{"product_id":product_id,
                            "product_name":product_name,
                            "quantity":quantity,
                            "timestamp":datetime.now()}})
    else:
        product_id = random.choice(inv_products_id)
        print(product_id)
        index = inv_products_id.index(product_id)
        product_name = inv_products_name[index]
        quantity = random.randint(10,20)

        return ({"event_type":event_type,
                 "product":{"product_id":product_id,
                            "product_name":product_name,
                            "quantity":quantity,
                            "timestamp":datetime.now()}})

if __name__ =='__main__':
    
    kinesis = boto3.client("kinesis")
    try:
        while True:
            prod_df = pd.read_csv('product_data.csv')
            inv_products_id = prod_df['product_id'].to_list()
            inv_products_name = prod_df['product_name'].to_list()
            print(prod_df)
            print(f"Product ID list {inv_products_id}")
            print(f"Product name list {inv_products_name}")
            data = data_generator(inv_products_id,inv_products_name)

            print("Data to send:")
            print(data)
            data["product"]["timestamp"] = data["product"]["timestamp"].isoformat()
            #kinesis code 
            response = kinesis.put_record(StreamName = 'inventory_stream', Data = json.dumps(data),
                                          PartitionKey="AdjustAsNeeded")
            print(f"Response: {response}")
            
            time.sleep(15)
    except KeyboardInterrupt:
        print("\n Script stopped by manual intervention")