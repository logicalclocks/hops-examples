# Running Kafka Examples
HopsWorks allows users to create their own Spark and Flink programs to produce and consume Avro records from Kafka. Users can follow the examples available in this project, modify them and submit use them in their own Projects. Instructions on how to do this are available in the next sections. 

## Flink & Kafka
To help you get started, *FlinkKafkaStreamingExample* provides the code for a basic streaming Flink application. To use it you need to provide the following parameters in the folling way when creating a Flink job in HopsWorks:
```
Usage: --topic <mytopic> --type <producer/consumer> --sink_path <path_to_dataset> --domain <domain> --url <url>
```
* **Topic**: The name of the Kafka topic where the job will produce or consume.
* **Type**: Defines if the the job is producing or consuming.
* **Sink_path**: This parameters is used only by a Consuming job and defines the path to the Dataset in which the Flink RollingSink writes its files. The latter contain the consumed Avro records from Kafka. In this example, the RollingSink creates a new folder (Bucket) every minute.
* **Domain**: The domain of your hopsworks installation. If running on hops.site, it should be set to "hops.site"
* **URL**: The URL of your local HopsWorks installation. If running on hops.site, it should be set to "https://hops.site:443" 

For examples on customizing logging for Flink jobs on HopsWorks see [here](https://github.com/hopshadoop/hops-kafka-examples/tree/master/examples-flink) 

*FlinkKafkaStreamingExample* makes use of the HopsWorks Kafka Utility available [here](https://github.com/hopshadoop/kafka-util). When running a Flink job, this utility is automatically available to your job, so there is no need to add it as an external library to your job.
