#Job Logging

##Flink Logs
JobManager and TaskManager logs are displayed in the *Jobs* tab in HopsWorks. For example, when printing a Flink *DataStream* object by doing
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
