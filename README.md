#  Hops Examples 

This repository provides users with examples on how to program Big Data and Deep Learning applications that run on [HopsWorks](https://github.com/hopshadoop/hopsworks), using [Apache Spark](https://spark.apache.org/), [Apache Flink](https://flink.apache.org/), [Apache Kafka](https://kafka.apache.org/),  [Apache Hive](https://hive.apache.org/)  and [TensorFlow](https://www.tensorflow.org/). Users can then upload and run their programs and notebooks from within their HopsWork projects. 

## Online Documentation ![N|Solid](http://www.hops.io/sites/default/files/hops-50x50.png)
You can find the latest Hops documentation on the [project's webpage](https://hops.readthedocs.io/en/latest/), including HopsWorks user and developer guides as well as a list of versions for all supported services. This README file is meant to provide basic instructions and codebase on how to build and run the examples.  

# Building the examples

```
mvn package
```

Generates a jar for each module which can then either be used to create HopsWorks jobs (Spark/Flink) or execute Hive queries remotely.

# Helper Libraries
Hops Examples makes use of **HopsUtil**, a set of Java and Python libraries which provide developers with tools that make programming on Hops easy. *HopsUtil* is automatically made available to all Jobs and Notebooks, without the user having to explicitely import it. Detailed documentation on HopsUtil is available [here](https://github.com/hopshadoop/hops-util). 

# Spark
## Structured Streaming with Kafka and HopsFS
To help you get started, [StructuredStreamingKafka.java](https://github.com/hopshadoop/hops-examples/blob/master/spark/src/main/java/io/hops/examples/spark/kafka/StructuredStreamingKafka.java) show how to build a Spark application that produces and consumes messages from Kafka and also persists it both in [Parquet](https://parquet.apache.org/) format and in plain text to HopsFS. The example makes use of the latest Spark-Kafka [API](https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html). To run the example, you need to provide the following parameters when creating a Spark job in HopsWorks:

```
Usage: <type>(producer|consumer)
```
* **type**: Defines if the the job is producing/consuming to/from Kafka.
* **sink**: Used only by a Consumer job, it defines the path to the Dataset or folder to which the Spark job appends its streaming output. The latter contain the consumed Avro records from Kafka. The name of the folder is suffixed with the YARN applicationId to deferantiate between multiple jobs writing to the same Dataset. In this example, the sink file contains data from the latest microbatch. The default microbatch period is set to two(2) seconds.

**MainClass** is io.hops.examples.spark.kafka.StreamingExample

**Topics** are provided via the HopsWorks Job UI. User checks the *Kafka* box and selects the topics from the drop-down menu. When consuming from multiple topics using a single Spark directStream, all topics must use the same Avro schema. Create a new directStream for topic(s) that use different Avro schemas.

**Consumer groups** are an advanced option for consumer jobs. A default one is set by HopsWorks and a user can add further ones via the Jobservice UI. 

Data consumed is be default persisted to the `Resources` dataset of the Project where the job is running.

### Avro Records
*StructuredStreamingKafka.java* generates *String <key,value>* pairs which are converted by **HopsUtil** into Avro records and serialized into bytes. Similarly, during consuming from a Kafka source, messages are deserialized into Avro records. **The default Avro schema used is the following**:

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
Hops Example provides Jupyter notebooks for running TensorFlow applications on Hops. All notebooks are automatically made available to HopsWorks projects upon project creation. Detailed documentation on how tp program TensorFlow on Hops, is available [here](https://hops.readthedocs.io/en/latest/user_guide/hopsworks/tensorflow.html).


# Hive
**HiveJDBCClient.java** available in hops-examples-hive, shows how users can remotely execute Hive queries against their HopsWorks projects' Hive databases. Firstly, it instantiates a Java JDBC client and then connects to the example database described in [Hops documentation](https://hops.readthedocs.io/en/latest/user_guide/hopsworks/hive.html#try-it-out). Users need to have created the database in their project as described in the documentation. This example uses [log4j2](https://logging.apache.org/log4j/2.x/) with logs being written to a `./hive/logs` directory. For changes made to `./hive/src/main/resources/log4j2.properties` to take effect, users must first do
```
mvn clean package
```

For *HiveJDBCClient.java* to be able to connect to the HopsWorks Hive server, users need to create a `hive_credentials.properties` file based on `hive_credentials.properties.example` and set proper values for the parameters:
```
hive_url=jdbc:hive2://[domain]:[port]
dbname=[database_name]
truststore_path=[path_to_truststore]
truststore_pw=[truststore_password]
hopsworks_username=[hopsworks_username]
hopsworks_pw=[hopsworks_password]
```

Users can export the project's certificates by navigating to its *Settings* page in HopsWorks. An email is then sent with the password for the truststore. 

# Flink
## Writing a Flink-Kafka Streaming Application
To help you get started, *StreamingExample* provides the code for a basic streaming Flink application. To use it you need to provide the following parameters when creating a Flink job for Kafka in HopsWorks:

```
Usage: -type <producer|consumer> [-sink_path <rolling_sink path>] [-batch_size <rolling_file_size>] [-bucket_format <bucket_format>]
```
* **type**: Defines if the the job is producing or consuming.
* **sink_path**: Used only by a Consumer job, itdefines the path to the Dataset in which the Flink RollingSink writes its files. The latter contain the consumed Avro records from Kafka. In this example, the RollingSink creates a new folder (Bucket) every minute.
* **batch_size**: Used only by a Consumer job, it defines the size of the file being written by the RollingSink. default is 32KB
* **bucket_format**: Used only by a Consumer job, it defines the names and creation frequency of the folders under sink_path. For more information see [DateTimeBucketer](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/connectors/fs/DateTimeBucketer.html) 

## Example:
(*A single space must exist between parameters*)

**Producer**

```
-type producer

```

**Consumer** 
```
-type consumer -sink_path /Projects/FlinkKafka/SinkE -batch_size 16 -bucket_format yyyy-MM-dd--HH
```
**Topic names** are provided via the HopsWorks Jobs user interface, when creating the job.

**Consumer groups** are provided via the HopsWorks Job UI. User provides a comma-separated list of the groups that shall be available within the application. 

Example
```
mytopic:yourtopic
```

## Avro Records
This example streams Tuples of String <key,value> pairs which are then serialized by the HopsAvroSchema class into Avro records and then produced to Kafka. The user needs to use a Tuple with twice as many fields as his schema (in this case Tuple4) which is required due to the Tuple containing *both the key and values* of the record. **The Avro schema used in this example is the following**:

```json
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

## Notes
1. Currently *Flink version 1.1.3* is supported.

2. For examples on customizing logging for Flink jobs on HopsWorks see [here](https://github.com/hopshadoop/hops-kafka-examples/tree/master/examples-flink).

3. *StreamingExample* makes use of [here](https://github.com/hopshadoop/hops-util). When building this project, HopsUtil is automatically included in the assembled jar file.


## Job Logging

### Flink Logs
JobManager and TaskManager logs are displayed in the *Jobs* tab in HopsWorks. For example, when printing a Flink *DataStream* object by doing

```java
DataStream<Tuple2<Tuple2<Integer, Integer>, Integer>> numbers = step.select("output")
				.map(new OutputMap());
numbers.print();
```
the DataStream output will be available in the HopsWorks Job Execution Logs. 

In case you would like this output to be written to a particular file in your Data Sets, you can do the following

```java
DataStream<Tuple2<Tuple2<Integer, Integer>, Integer>> numbers = step.select("output")
				.map(new OutputMap());
numbers.writeAsText("absolute_path_to_file");
```

the DataStrem object will be printed into the file specified in the *writeAsText* method. The path must point to either the *Logs* or *Resources* data sets in HopsWorks. 
For example, if you would like the output of a the Flink DataStream object to be written to a *test.out* file in your Resources data set, the command is the following

```java
numbers.writeAsText("/Projects/myproject/Resources/test.out");
```

The path to an existing file can be easily found by clicking on this particular file in HopsWorks-DataSets and then easily copy the path by using the clipboard icon.


### Application Logs
In case you want to print something to the *standard output* within your application, *do not use System.out*. Instead, use the following code example which utilizes an HDFS client to 
write your output to HDFS. The file path must be set similarly to the Flink logs described above.
```java
Configuration hdConf = new Configuration();
Path hdPath = new org.apache.hadoop.fs.Path("/Projects/myproject/Logs/mylog/test3.out");
FileSystem hdfs = hdPath.getFileSystem(hdConf);
FSDataOutputStream stream = hdfs.create(hdPath);
stream.write("My first Flink program on Hops!".getBytes());
stream.close();
``` 