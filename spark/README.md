# Spark & Kafka
To help you get started, *StreamingExample* provides the code for a basic streaming Spark application. HopsWorks makes use of the latest Spark-Kafka experimental [API](http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html). To run the example you need to provide the following parameters when creating a Spark job for Kafka in HopsWorks:
```
Usage: <type>(producer|consumer) [<sink>]
```
* **type**: Defines if the the job is producing/consuming to/from Kafka.
* **sink**: Used only by a Consumer job, it defines the path to the Dataset or folder to which the Spark job appends its streaming output. The latter contain the consumed Avro records from Kafka. The name of the folder is suffixed with the YARN applicationId to deferantiate between multiple jobs writing to the same Dataset. In this example, the sink file contains data from the latest microbatch. The default microbatch period is set to two(2) seconds.

**MainClass** is io.hops.examples.spark.kafka.StreamingExample

**Topics** are provided via the HopsWorks Job UI. User checks the *Kafka* box and provides the topic names as comma separated value.When consuming from multiple topics using a single Spark directStream, all topics must use the same Avro schema. Create a new directStream for topic(s) that use different Avro schemas.


## Example:
**Producer**

```
producer

```

**Consumer** 
```
consumer /Projects/KafkaProject/Resources/Data
```

## Avro Records
This example produces String <key,value> pairs which are converted by HopsWorks **KafkaUtil** into Avro records and serialized into bytes. Similarly, during consuming from a Spark directStream, messages are deserialized into Avro records. **The Avro schema used in this example is the following**:

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

## Libraries

*StreamingExample* makes use of the HopsWorks Kafka Utility available [here](https://github.com/hopshadoop/kafka-util). Add this library to your job when creating it in HopsWorks.
