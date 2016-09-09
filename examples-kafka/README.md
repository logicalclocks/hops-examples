# Running Kafka Examples
HopsWorks allows users to create their own Apache Spark and Apache Flink programs to produce and consume Avro records from Kafka. Users can follow the examples available in this project, modify them and submit use them in their own Projects. Instructions on how to do this are available in the next sections. 

## Flink & Kafka
To help you get started, *FlinkKafkaStreamingExample* provides the code for a basic streaming Flink application. To use it you need to provide the following parameters in the folling way when creating a Flink job in HopsWorks:
```
Usage: --topic <mytopic> --type <producer/consumer> --sink_path <path_to_dataset> --domain <domain> --url <url>
```
* **Topic**: The name of the Kafka topic where the job will produce or consume.
* **Type**: Defines if the the job is producing or consuming.
* **Sink_path**: Used only by a Consumer job, itdefines the path to the Dataset in which the Flink RollingSink writes its files. The latter contain the consumed Avro records from Kafka. In this example, the RollingSink creates a new folder (Bucket) every minute.
* **Domain**: The domain of your hopsworks installation. If running on hops.site, it should be set to "hops.site"
* **URL**: The URL of your local HopsWorks installation. If running on hops.site, it should be set to "https://hops.site:443" 
* **Batch_size**: Used only by a Consumer job, it defines the size of the file being written by the RollingSink. default is 32KB

#### Example:
(*A single space must exist between parameters*)

**Producer**

```
--topic mytopic --type producer --domain localhost --url http://localhost:8080
```

**Consumer** 
```
--topic mytopic --type consumer --sink_path /Projects/FlinkKafka/Resources --domain localhost --url http://localhost:8080 --batch_size 32
```

#### Avro Records
This example streams Tuples of String <key,value> pairs which are then serialzied by the HopsAvroSchema class into Avro records and then sent to Kafka. The user needs to use a Tuple with twice as many fields as his schema (in this case Tuple4) which is done because the Tuple will contain both key and values of the record. **The Avro schema used in this example is the following**:

```
{
    "fields": [
        { "name": "platform", "type": "string" },
        { "name": "program", "type": "string" }
    ],
    "name": "myrecord",
    "type": "record"
}
```
For Avro schemas with more fields, the application's SourceFunction should use a Tuple with the proper arity and the user should also update the definition of the *HopsAvroSchema* class accordingly. No other change is required by this class.
#### Notes
1. Currently Flink 1.0.3 is supported, however an upgrade to 1.1.2 is in the short-term roadmap. *Upgrading means the Avro mechanism in HopsWorks is bound to change in order to make use of the new classes available in the latest Apache Flink distribution.*

2. For examples on customizing logging for Flink jobs on HopsWorks see [here](https://github.com/hopshadoop/hops-kafka-examples/tree/master/examples-flink) 

3. *FlinkKafkaStreamingExample* makes use of the HopsWorks Kafka Utility available [here](https://github.com/hopshadoop/kafka-util). When running a Flink job, this utility is automatically available to your job, so there is no need to add it as an external library to your job.
