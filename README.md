# simple-streaming-pipeline

This is a simulation of a simple real-time streaming pipeline.

Prerequisites:
docker  
kafka-python  
pyspark  

You can choose to run it entirely inside a container.  
I have chosen to run the kafka server and postgres inside the container, while simulating producer and consumer outside the container.  

1. Clone the repository  
2. cd into the repo  
3. `docker-compose up -d`  
5. This should have the kafka bootstrap servers and postgres up.  
6. `python kafka_producer.py`  
7. In a different terminal, `python spark_consumer.py`  
8. We can then use this data from postgres downstream, for example a Tableau dashboard.  

