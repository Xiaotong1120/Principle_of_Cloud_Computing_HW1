# Kafka IoT Data Pipeline with Machine Learning Inference

## Overview

This project implements an IoT data pipeline using **Kafka** for real-time image streaming, **PyTorch** for machine learning inference, and **CouchDB** for data storage. The system consists of multiple virtual machines (VMs) working together to process, classify, and store images from the CIFAR-10 dataset in real-time.

### Key Features:
- **VM1**: Acts as an IoT data producer, sending CIFAR-10 images to a Kafka broker every 10 seconds.
- **VM2**: Acts as the Kafka broker, facilitating message transmission between producer and consumers.
- **VM3**: Acts as a consumer, performing image classification (machine learning inference) on received images and sending the predictions to a Kafka topic.
- **VM4**: Listens for both raw images and prediction results, storing them in **CouchDB**.

### Tools & Libraries:
- **Apache Kafka**: For message streaming between VMs.
- **PyTorch**: For image classification using pre-trained models.
- **CouchDB**: A NoSQL database to store image data and classification results.
- **Python Libraries**:
  - `kafka-python`: Kafka producer and consumer.
  - `torch` and `timm`: For machine learning inference.
  - `Pillow`: For image processing.
  - `base64`: For image encoding/decoding.
  - `couchdb`: For interacting with the CouchDB database.
  
### Machine Learning Model

For image classification, we use a pre-trained **ResNet-20** model from **PyTorch Hub**, specifically designed for the **CIFAR-10** dataset. The model is loaded using the following command:

```python
model = torch.hub.load('chenyaofo/pytorch-cifar-models', 'cifar10_resnet20', pretrained=True)

**ResNet-20** is a well-optimized model for CIFAR-10 and provides a good balance between computational efficiency and accuracy for small image sizes like 32x32 pixels. It utilizes residual connections to mitigate the vanishing gradient problem commonly found in deep networks.

## Why ResNet-20?

- **Tailored for CIFAR-10**: This model is specifically designed and trained for the CIFAR-10 dataset, making it highly suitable for classifying its small image sizes.
- **Computational Efficiency**: ResNet-20 offers a good balance between accuracy and computational speed.
- **Residual Connections**: These connections allow the model to train deeper networks effectively without running into the vanishing gradient problem.

Using this pre-trained model allows us to achieve reasonable classification performance without needing extensive training on the CIFAR-10 dataset.

## Architecture Overview
1. **VM1**: Sends images to the `iot-topic` in Kafka.
2. **VM2**: Acts as the Kafka broker, routing messages from producers to consumers.
3. **VM3**: Consumes images from `iot-topic`, processes them through a machine learning model, and sends predictions to `iot-predictions`.
4. **VM4**: Consumes both `iot-topic` (for raw images) and `iot-predictions` (for classification results), storing both in CouchDB.

## Deployment Instructions

### Prerequisites
This system is designed to be deployed on the provided virtual machines (VM1, VM2, VM3, VM4). You do not need to install any software locally; instead, you will use the configured virtual machines for all components.

### Deployment Steps

## Step 1: Start Zookeeper and Kafka Broker (VM2)

1. SSH into VM2.
2. Locate to the path '/home/cc/kafka_2.13-3.8.0'
3. Start the Zookeeper service:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

4. Once Zookeeper is running, start the Kafka broker:

```bash
bin/kafka-server-start.sh config/server.properties
```

This will start the Zookeeper and Kafka broker, which will act as the central message hub for the pipeline.

## Step 2: Start Consumers (VM3 and VM4)

### VM3:

1. SSH into VM3.
2. Locate to the path '/home/cc/HW1'
3. Start the consumer that performs machine learning inference on received images:

```bash
python iot_consumer.py
```

This will consume messages from the `iot-topic`, process them through a machine learning model, and send predictions to `iot-predictions`.

### VM4:

1. SSH into VM4.
2. Locate to the path '/home/cc/HW1'
3. Start the consumer that listens to both `iot-topic` (for raw images) and `iot-predictions` (for classification results):

```bash
python iot_data_consumer.py
```

This will store image data and update the records with the classification results in CouchDB.

## Step 3: Start Producer (VM1)

1. SSH into VM1.
2. Locate to the path '/home/cc/HW1'
3. Start the Kafka producer that sends CIFAR-10 images to the `iot-topic` every 10 seconds:

```bash
python iot_producer.py
```

This will generate a unique ID for each image, convert it to base64, and send it to the Kafka broker.

## Step 4: Monitor CouchDB (VM4)

On VM4, you can verify that images and their corresponding predictions are being stored in CouchDB:

```bash
curl -X GET http://admin:admin@127.0.0.1:5984/img_db/_all_docs
```

To view a specific image document:

```bash
curl -X GET http://admin:admin@127.0.0.1:5984/img_db/<IMAGE_ID>
```

### Verification

- **Image Data**: Check that raw image data and the GroundTruth label are stored in CouchDB.
- **Prediction Results**: Ensure that the prediction results (inferred values) are updated in CouchDB for each image.

## Notes

This pipeline is designed for real-time processing. 
The Kafka producer sends images every 10 seconds during a minute (so 6 times in total) , and the consumers process the data in near real-time. 
CouchDB is used to store the entire history of received images and their associated predictions.

