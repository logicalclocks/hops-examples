#  Hops Examples

This repository provides users with examples on how to program Big Data and Deep Learning applications that run on [Hopsworks](https://github.com/logicalclocks/hopsworks), using [Apache Spark](https://spark.apache.org/), [Apache Flink](https://flink.apache.org/), [Apache Kafka](https://kafka.apache.org/),  [Apache Hive](https://hive.apache.org/)  and [TensorFlow](https://www.tensorflow.org/). Users can then upload and run their programs and notebooks from within their Hopsworks projects.

## Online Documentation
You can find the latest Hopsworks documentation on the [project's webpage](https://hopsworks.readthedocs.io/en/latest/), 
including Hopsworks user and developer guides as well as a list of versions for all supported services. This README file is meant to provide basic instructions and codebase on how to build and run the examples.

# Website Generation (Hugo)

Install dependencies first:

    pip3 install jupyter
    pip3 install nbconvert


Generate the webpages and run the webserver:

    export LC_CTYPE=en_US.UTF-8
    python3 make.py
    ./bin/hugo server

When you add a new notebook, add it under the "notebooks" directory.
If you want to add a new category for notebooks, put your notebook in a new directory, then edit this file to add your category:

    themes/berbera/layouts/index.html


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

# Feature Store

A sample feature engineering job that takes in raw data, transforms it into features suitable for machine learning 
and saves the features into the featurestore is available in `featurestore/`. This job will automatically be 
available in your project if you take the **featurestore tour** on Hopsworks. Example notebooks for interacting with 
the featurestore are available in `notebooks/featurestore/`. More documentation about the featurestore is available 
here: [Featurestore Documentation](https://hopsworks.readthedocs.io/en/latest/user_guide/hopsworks/featurestore.html).

API documentation is available here: [Featurestore API Docs](http://hops-py.logicalclocks.com/).



# TensorFlow Extended (TFX)

This repo comes with notebooks demonstrating how to implement horizontally scalable TFX pipelines. The 
``chicago_taxi_tfx_hopsworks`` notebook contains all the steps of the pipeline along with visualizations. It is based
on the TFX Chicago taxi rides example but uses a smaller slice of the original dataset. The notebook downloads the 
dataset into Hopsworks and then calls the TFX components to go all the way from data preparation to model analysis.
 
That notebook then is split into smaller ones that correspond to the different steps in the pipeline. These notebooks
can be found under the ``notebookstfx/chicago_taxi/pipeline`` directory in this repo. To execute these, you need to 
create one Hopsworks Spark job per notebook and then use the Apache Airflow dag ``chicago_tfx_airflow_pipeline.py`` 
provided in this repo to orchestrate them. Please refer to the Apache Airflow section of the user guide on how to 
upload manage your dags. There is also a ``Visualizations`` notebook that runs the visualizations steps of the 
pipeline and can be executed at any time, as the output of the pipeline (statistics, schema, etc.) is persisted to 
the ``Resources`` dataset in your project. You can catch a demo of the pipeline [here](https://www.youtube.com/watch?v=v1DrnY8caVU).

# Beam

Under ``notebooks/beam`` you can find the ``portability_wordcount_python`` notebook, which guides you through running
a WordCount program in a Python Portable Beam Pipeline. You can download the notebook from this repo, upload it in 
your Hopsworks project and just run it! Hopsworks transparently manages the entire lifecycle of the notebook and the 
Beam related services and components.


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
