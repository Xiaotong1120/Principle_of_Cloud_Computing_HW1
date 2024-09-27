import json
import base64
from kafka import KafkaConsumer, KafkaProducer
from PIL import Image
import torch
import torchvision.transforms as transforms
from io import BytesIO

# CIFAR-10 class names
CIFAR10_LABELS = [
    'airplane', 'automobile', 'bird', 'cat', 'deer', 
    'dog', 'frog', 'horse', 'ship', 'truck'
]

# Step 1: Set up Kafka consumer for 'iot-topic'
consumer = KafkaConsumer(
    'iot-topic',
    bootstrap_servers=["192.168.5.17"],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='ml-inference-group'
)
print("Kafka consumer initialized successfully.")

# Step 2: Set up Kafka producer for sending predictions to 'iot-predictions'
producer = KafkaProducer(
    bootstrap_servers=["192.168.5.17"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print("Kafka producer for predictions initialized successfully.")

# Step 3: Load pre-trained ResNet model for CIFAR-10 using torch.hub
model = torch.hub.load('chenyaofo/pytorch-cifar-models', 'cifar10_resnet20', pretrained=True)
model.eval()

# Define transformation to preprocess images for CIFAR-10
preprocess = transforms.Compose([
    transforms.Resize((32, 32)),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.4914, 0.4822, 0.4465], std=[0.2023, 0.1994, 0.2010]),
])

# Step 4: Function to perform image inference
def infer_image(image_base64):
    # Decode base64 image
    image_data = base64.b64decode(image_base64)
    image = Image.open(BytesIO(image_data)).convert("RGB")

    # Apply preprocessing
    input_tensor = preprocess(image).unsqueeze(0)  # Add batch dimension
    with torch.no_grad():
        output = model(input_tensor)
        _, predicted_index = output.max(1)

    predicted_label = CIFAR10_LABELS[predicted_index.item()]
    return predicted_index.item(), predicted_label

# Step 5: Function to send prediction result to Kafka ('iot-predictions')
def send_prediction_to_kafka(image_id, predicted_label):
    prediction_data = {
        "ID": image_id,
        "InferredValue": predicted_label
    }
    producer.send('iot-predictions', value=prediction_data)
    producer.flush()  # Ensure data is sent
    print(f"Sent prediction for image ID {image_id} to Kafka: {predicted_label}")

# Step 6: Consume messages from Kafka, perform inference, and send result
try:
    for message in consumer:
        data = message.value
        print(f"Received data from Kafka with ID: {data['ID']}")

        # Get the base64 image data
        image_base64 = data['Data']

        # Perform inference
        predicted_index, predicted_label = infer_image(image_base64)
        print(f"Predicted class for image with ID {data['ID']}: {predicted_label} (Index: {predicted_index})")
        
        # Send the prediction result to Kafka ('iot-predictions')
        send_prediction_to_kafka(data['ID'], predicted_label)

except Exception as e:
    print(f"Error consuming message or performing inference: {e}")
finally:
    consumer.close()
    producer.close()
    print("Kafka consumer and producer closed.")