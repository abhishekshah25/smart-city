## Smart City Data Pipeline

### System Components

1. docker-compose.yml - It creates the environment in which the kafka broker, zookeeper and spark nodes are hosted.

2. main.py - It has the necessary functions surrounding the creation of incoming data as well as create the Kafka topic and produce data on to the topic.

3. spark-city.py - It contains the logic to consume the data from the Kafka topic and stream it to the Amazon S3 buckets.

Furthermore, the data pushed into the AWS S3 buckets is transformed using AWS Glue Crawler and viewed using Amazon Athena. This data is then written into Amazon Redshift using the AWS Glue Data Catalog.
