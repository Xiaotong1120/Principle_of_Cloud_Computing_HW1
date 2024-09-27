import uuid
import json
import time
from kafka import KafkaProducer
from PIL import Image
import base64
import pickle
import numpy as np
from io import BytesIO
import random

# Initialize Kafka producer with acks and additional debugging
try:
    producer = KafkaProducer(
        bootstrap_servers='192.168.5.17', 
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=1,  # Acknowledgment to ensure message delivery
        api_version=(0, 10, 1)  # Specify Kafka API version if necessary
    )
    print("Kafka producer initialized successfully.")
except Exception as e:
    print(f"Failed to initialize Kafka producer: {e}")

# Path to the unzipped CIFAR-10 dataset
cifar10_path = './cifar-10-batches-py/'

# Load CIFAR-10 binary files
def unpickle(file):
    try:
        with open(file, 'rb') as fo:
            data_dict = pickle.load(fo, encoding='bytes')
        print(f"Loaded CIFAR-10 data from {file}")
        return data_dict
    except Exception as e:
        print(f"Error loading CIFAR-10 data from {file}: {e}")

# Load CIFAR-10 meta file to get correct label names
def load_label_names():
    try:
        with open(cifar10_path + 'batches.meta', 'rb') as f:
            label_data = pickle.load(f, encoding='bytes')
        label_names = [label.decode('utf-8') for label in label_data[b'label_names']]
        print(f"Loaded CIFAR-10 label names: {label_names}")
        return label_names
    except Exception as e:
        print(f"Error loading label names: {e}")
        return []

# Load correct label names from the dataset
label_names = load_label_names()

# Load the data from CIFAR-10 dataset batch
batch = unpickle(cifar10_path + 'data_batch_1')
images = batch[b'data']  # Image data
labels = batch[b'labels']  # Corresponding labels

# CIFAR-10 images are 32x32 pixels, we need to convert them to proper image format
def convert_image(image_data):
    try:
        image_data = np.reshape(image_data, (3, 32, 32)).transpose(1, 2, 0)  # Reshape to 32x32x3 image
        image = Image.fromarray(image_data)
        print("Image converted successfully.")
        return image
    except Exception as e:
        print(f"Error converting image: {e}")

# Function to send image to Kafka with error handling
def send_image_to_kafka(image_data, label):
    try:
        # Generate a unique UUID for each image
        unique_id = str(uuid.uuid4())
        print(f"Generated unique ID: {unique_id}")
        
        # Convert image to PIL object (no blur effect)
        image = convert_image(image_data)
        
        # Convert image to base64 string (as PNG)
        buffered = BytesIO()
        image.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode('utf-8')
        print("Image converted to base64.")

        # Create the JSON object as per the assignment requirements
        data = {
            'ID': unique_id,  # Unique ID
            'GroundTruth': label,  # Image label
            'Data': img_str  # Image data
        }
        print(f"Prepared data for Kafka: {data}")

        # Send the data to Kafka broker
        producer.send('iot-topic', value=data)
        producer.flush()  # Ensure data is sent
        print(f"Sent image with label {label} to Kafka.")
    
    except Exception as e:
        print(f"Failed to send image to Kafka: {e}")

# Send 6 images at 10-second intervals
try:
    for _ in range(6):  # Loop 6 times
        random_index = random.randint(0, len(images) - 1)  # Choose a random index from the dataset
        image_data = images[random_index]  # Get a random image from the dataset
        image_label = label_names[labels[random_index]]  # Get the corresponding label
        send_image_to_kafka(image_data, image_label)  # Send the image and label
        print(f"Random image at index {random_index} sent successfully.")
        time.sleep(10)  # Wait for 10 seconds before sending the next image
except Exception as e:
    print(f"Error in sending image: {e}")
finally:
    producer.close()
    print("Kafka producer closed.")