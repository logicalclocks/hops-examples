# Quickstart

HopsWorks enables users to create their own Apache Spark and Apache Flink programs to produce and consume Avro records from Kafka. Users can follow the examples available in this project, modify them and finally use them in their own HopsWorks projects. Instructions on how to do this are available in the next sections. 


For examples on writing Spark and Flink streaming application with Kafka, see README files in [spark](https://github.com/hopshadoop/hops-examples/blob/master/spark/README.md) and [flink](https://github.com/hopshadoop/hops-examples/blob/master/flink/README.md) directories respectively.

#Build
```
mvn package
```
Generates a jar for each job type which can be used in HopsWorks projects.
