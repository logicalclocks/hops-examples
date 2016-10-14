# Running Kafka Examples
HopsWorks allows users to create their own Apache Spark and Apache Flink programs to produce and consume Avro records from Kafka. Users can follow the examples available in this project, modify them and submit use them in their own Projects. Instructions on how to do this are available in the next sections. 

# Flink
To help you get started, *FlinkKafkaStreamingExample* provides the code for a basic streaming Flink application. To use it you need to provide the following parameters in the folling way when creating a Flink job in HopsWorks:
```
Usage: -topic <topic_name> -type <producer|consumer> [-sink_path <rolling_sink path>] [-batch_size <rolling_file_size>] [-bucket_format <bucket_format>]
```
* **topic**: The name of the Kafka topic where the job will produce or consume.
* **type**: Defines if the the job is producing or consuming.
* **sink_path**: Used only by a Consumer job, itdefines the path to the Dataset in which the Flink RollingSink writes its files. The latter contain the consumed Avro records from Kafka. In this example, the RollingSink creates a new folder (Bucket) every minute.
* **batch_size**: Used only by a Consumer job, it defines the size of the file being written by the RollingSink. default is 32KB
* **bucket_format**: Used only by a Consumer job, it defines the names and creation frequency of the folders under sink_path. For more information see [DateTimeBucketer](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/connectors/fs/DateTimeBucketer.html) 

#### Example:
(*A single space must exist between parameters*)

**Producer**

```
-topic mytopic -type producer

```

**Consumer** 
```
-topic mytopic -type consumer -sink_path /Projects/FlinkKafka/SinkE -batch_size 16 -bucket_format yyyy-MM-dd--HH
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


#Job Logging

##Flink Logs
JobManager and TaskManager logs are displayed in the *Jobs* tab in HopsWorks. For example, when printing a Flink *DataStream* object by doing
```
DataStream<Tuple2<Tuple2<Integer, Integer>, Integer>> numbers = step.select("output")
				.map(new OutputMap());
numbers.print();
```
the DataStream output will be available in the HopsWorks Job Execution Logs. 

In case you would like this output to be written to a particular file in your Data Sets, you can do the following

```
DataStream<Tuple2<Tuple2<Integer, Integer>, Integer>> numbers = step.select("output")
				.map(new OutputMap());
numbers.writeAsText("absolute_path_to_file");
```

the DataStrem object will be printed into the file specified in the *writeAsText* method. The path must point to either the *Logs* or *Resources* data sets in HopsWorks. 
For example, if you would like the output of a the Flink DataStream object to be written to a *test.out* file in your Resources data set, the command is the following

```
numbers.writeAsText("hdfs://10.0.2.15:8020/Projects/myproject/Resources/test.out");
```

The path to an existing file can be easily found by clicking on this particular file in HopsWorks-DataSets and then easily copy the path by using the clipboard icon.


##ApplicationLogs
In case you want to print something to the *standard output* within your application, *do not use System.out*. Instead, use the following code example which utilizes an HDFS client to 
write your output to HDFS. The file path must be set similarly to the Flink logs described above.
```
Configuration hdConf = new Configuration();
Path hdPath = new org.apache.hadoop.fs.Path("hdfs://10.0.2.15:8020/Projects/myproject/Logs/mylog/test3.out");
FileSystem hdfs = hdPath.getFileSystem(hdConf);
FSDataOutputStream stream = hdfs.create(hdPath);
stream.write("My first Flink program on Hops!".getBytes());
stream.close();
``` 
