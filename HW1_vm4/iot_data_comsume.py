import os
import json
from kafka import KafkaConsumer
import couchdb
import threading

# Step 1: Connect to CouchDB server
try:
    couch = couchdb.Server('http://admin:admin@127.0.0.1:5984/')
    db = couch['img_db']
    print("Connected to CouchDB successfully.")
except Exception as e:
    print(f"Failed to connect to CouchDB: {e}")

# Step 2: Initialize Kafka Consumer for 'iot-topic' (for image data)
try:
    consumer_iot = KafkaConsumer(
        'iot-topic',  # Topic to listen to for image data
        bootstrap_servers=["192.168.5.17"],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='iot-consumer-group'
    )
    print("Kafka consumer for 'iot-topic' initialized successfully.")
except Exception as e:
    print(f"Failed to initialize Kafka consumer for 'iot-topic': {e}")

# Step 3: Initialize Kafka Consumer for 'iot-predictions' (for inference results)
try:
    consumer_predictions = KafkaConsumer(
        'iot-predictions',  # Topic to listen to for inference results
        bootstrap_servers=["192.168.5.17"],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='iot-predictions-group'
    )
    print("Kafka consumer for 'iot-predictions' initialized successfully.")
except Exception as e:
    print(f"Failed to initialize Kafka consumer for 'iot-predictions': {e}")

# Step 4: Function to store image data from 'iot-topic'
def store_image_data_in_db(data):
    try:
        # Explicitly set the ID for the document
        doc = {
            "_id": data['ID'],
            "GroundTruth": data['GroundTruth'],
            "Data": data['Data']
        }
        db.save(doc)
        print(f"Stored image data with ID: {data['ID']}")
    except Exception as e:
        print(f"Error saving image data to CouchDB: {e}")

# Step 5: Function to update prediction result in CouchDB
# Function to update prediction result in CouchDB
def update_prediction_in_db(data):
    try:
        image_id = data['ID']  # The ID of the image document in CouchDB
        inferred_value = data['InferredValue']  # The predicted class
        
        # Check if the document exists in CouchDB
        if image_id in db:
            doc = db[image_id]
            doc['InferredValue'] = inferred_value  # Update with the inferred value
            db.save(doc)  # Save the updated document back to CouchDB
            print(f"Updated InferredValue for image ID: {image_id} to {inferred_value}")
        else:
            print(f"Document with ID {image_id} not found in CouchDB.")
    except Exception as e:
        print(f"Error updating prediction in CouchDB: {e}")

# Step 6: Function to consume messages from 'iot-topic' and store image data
def consume_iot_topic():
    try:
        for message in consumer_iot:
            image_data = message.value
            print(f"Received image data from Kafka: {image_data}")
            store_image_data_in_db(image_data)
    except Exception as e:
        print(f"Error consuming image data from 'iot-topic': {e}")
    finally:
        consumer_iot.close()

# Step 7: Function to consume messages from 'iot-predictions' and update prediction
def consume_predictions_topic():
    try:
        for message in consumer_predictions:
            prediction_data = message.value
            print(f"Received prediction result from Kafka: {prediction_data}")
            update_prediction_in_db(prediction_data)
    except Exception as e:
        print(f"Error consuming prediction data from 'iot-predictions': {e}")
    finally:
        consumer_predictions.close()

# Step 8: Use threads to consume from both topics simultaneously
thread_iot = threading.Thread(target=consume_iot_topic)
thread_predictions = threading.Thread(target=consume_predictions_topic)

# Start both threads
thread_iot.start()
thread_predictions.start()

# Wait for both threads to complete
thread_iot.join()
thread_predictions.join()

print("Kafka consumers closed.")