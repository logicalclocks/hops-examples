#  Hops Examples

This repository provides users with examples on how to program Big Data and Deep Learning applications that run on [Hopsworks](https://github.com/logicalclocks/hopsworks), using [Apache Spark](https://spark.apache.org/), [Apache Flink](https://flink.apache.org/), [Apache Kafka](https://kafka.apache.org/),  [Apache Hive](https://hive.apache.org/)  and [TensorFlow](https://www.tensorflow.org/). Users can then upload and run their programs and notebooks from within their Hopsworks projects.

## Online Documentation
You can find the latest Hopsworks documentation on the [project's webpage](https://hopsworks.readthedocs.io/en/latest/), 
including Hopsworks user and developer guides as well as a list of versions for all supported services. This README file is meant to provide basic instructions and codebase on how to build and run the examples.

# Building the examples

```
mvn package
```

Generates a jar for each module which can then either be used to create Hopsworks jobs (Spark/Flink) or execute Hive queries remotely.

# Helper Libraries
Hops Examples makes use of **Hops**, a set of Java and Python libraries which provide developers with tools that make programming on Hops easy. *Hops* is automatically made available to all Jobs and Notebooks, without the user having to explicitely import it. Detailed documentation on Hops is available [here](https://github.com/logicalclocks/hops-util).

# Spark
## Structured Streaming with Kafka and HopsFS
To help you get started, [StructuredStreamingKafka](https://github.com/logicalclocks/hops-examples/blob/master/spark/src/main/scala/io/hops/examples/spark/kafka/StructuredStreamingKafka.scala) show how to build a Spark application that produces and consumes messages from Kafka and also persists it 
both in [Parquet](https://parquet.apache.org/) format and in plain text to HopsFS. The example makes use of the latest Spark-Kafka [API](https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html). To run the example, you need to provide the following parameters when creating a Spark job in Hopsworks:

```
Usage: <type>(producer|consumer)
```
* **type**: Defines if the the job is producing/consuming to/from Kafka.
* **sink**: Used only by a Consumer job, it defines the path to the Dataset or folder to which the Spark job appends its streaming output. The latter contain the consumed Avro records from Kafka. The name of the folder is suffixed with the YARN applicationId to deferantiate between multiple jobs writing to the same Dataset. In this example, the sink file contains data from the latest microbatch. The default microbatch period is set to two(2) seconds.

**MainClass** is io.hops.examples.spark.kafka.StructuredStreamingKafka

**Topics** are provided via the Hopsworks Job UI. User checks the *Kafka* box and selects the topics from the drop-down menu. When consuming from multiple topics using a single Spark directStream, all topics must use the same Avro schema. Create a new directStream for topic(s) that use different Avro schemas.

Data consumed is be default persisted to the `Resources` dataset of the Project where the job is running.

### Avro Records
`StructuredStreamingKafka.java` generates *String <key,value>* pairs which are converted by **Hops** into Avro records and serialized into bytes. Similarly, during consuming from a Kafka source, messages are deserialized into Avro records. **The default Avro schema used is the following**:

```json
{
	"fields": [
		{
			"name": "timestamp",
			"type": "string"
		},
		{
			"name": "priority",
			"type": "string"
		},
		{
			"name": "logger",
			"type": "string"
		},
		{
			"name": "message",
			"type": "string"
		}
	],
	"name": "myrecord",
	"type": "record"
}
```


# TensorFlow
Hops Example provides Jupyter notebooks for running TensorFlow applications on Hops. All notebooks are automatically 
made available to Hopsworks projects upon project creation. Detailed documentation on how tp program TensorFlow on 
Hopsworks, is available [here](https://hopsworks.readthedocs.io/en/latest/user_guide/hopsworks/tensorflow.html).


# Hive
`HiveJDBCClient.java` available in `hops-examples-hive`      }
, shows how users can remotely execute Hive queries against their Hopsworks projects' Hive databases. Firstly, it 
instantiates a Java JDBC client and then connects to the example database described in
 [Hopsworks documentation](https://hops.readthedocs.io/en/latest/user_guide/hopsworks/hive.html#try-it-out). Users need to have created the database in their project as described in the documentation. This example uses [log4j2](https://logging.apache.org/log4j/2.x/) with logs being written to a `./hive/logs` directory. For changes made to `./hive/src/main/resources/log4j2.properties` to take effect, users must first do
```
mvn clean package
```

For `HiveJDBCClient.java` to be able to connect to the Hopsworks Hive server, users need to create a `hive_credentials.properties` file based on `hive_credentials.properties.example` and set proper values for the parameters:
```
hive_url=jdbc:hive2://[domain]:[port] #default port:9085
dbname=[database_name] #the name of the Dataset in Hopsworks, omitting the ".db" suffix.
truststore_path=[absolute_path_to_truststore]
keystore_path=[absolute_path_to_keystore]
truststore_pw=[truststore_password]
keystore_pw=[keystore_password]
```

Users can export their project's certificates by navigating to the *Settings* page in Hopsworks. An email is then sent
 with the password for the truststore and keystore.
