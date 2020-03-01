"""
featurestore_util.py
~~~~~~~~~~

This Python module contains utility functions for starting
Apache Spark jobs creating training datasets in the Hopsworks Feature Store.
It is primarily used for creating training datasets of formats 'petastorm', 'npy' and 'hdf5' as those
data formats are not supported in the Scala SDK. For other Feature Store Util operations see 'featurestoreutil4j'
"""

from hops import featurestore, hdfs
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import json
import argparse

def parse_args():
    """Parses the commandline arguments with argparse"""
    parser = argparse.ArgumentParser(description='Input command-line arguments to configure the util4j job')
    parser.add_argument("-i", "--input", help="path to JSON input arguments",
                        default="")
    args = parser.parse_args()
    return args


def setup_spark():
    """
    Setup the spark session

    Returns:
        sparksession, sparkContext, sqlContext
    """
    spark=SparkSession.builder.enableHiveSupport().getOrCreate()
    sc=spark.sparkContext
    sqlContext=SQLContext(sc)
    return spark, sc, sqlContext


def parse_input_json(hdfs_path):
    """
    Parse input JSON command line arguments for the util job

    Args:
        :hdfs_path: path to the JSON input on HDFS

    Returns:
        The parsed JSON (dict)
    """
    return json.loads(hdfs.load(hdfs_path))


def pre_process_features(features):
    """
    Pre process the features passed on command-line

    Args:
        :features: string with comma-separated features

    Returns:
        list of features
    """
    if(len(features) == 0):
        raise ValueError("features cannot be empty")
    features = list(map(lambda f: convert_dict_feature_to_name(f), features))
    return features


def convert_dict_feature_to_name(feature):
    if type(feature) is dict:
        return feature["name"]
    return feature


def pre_process_stat_columns(statcolumns):
    """
    Pre process stat columns passed on command line

    Args:
        :statcolumns_str: string of comma separated stat-columns

    Returns:
        the parsed list of stat columns
    """
    if(len(statcolumns) == 0):
        return None
    else:
        return statcolumns


def pre_process_featuregroups(featuregroups_versions):
    """
    Pre process the command-line input into a dict with fg-->version

    Args:
        :featuregroups_versions: feature groups json list

    Returns:
        the parsed dict with fg-->version
    """
    if(len(featuregroups_versions) == 0):
        raise ValueError("featuregroups cannot be empty")
    featuregroups_version_dict = {}
    for fg in featuregroups_versions:
        featuregroups_version_dict[fg["name"]] = fg["version"]
    return featuregroups_version_dict


def create_training_dataset(input_json):
    """
    Creates a training dataset based on command-line arguments

    Args:
        :input_json: input JSON arguments

    Returns:
        None
    """

    features = pre_process_features(input_json["features"])
    featuregroups_version_dict = pre_process_featuregroups(input_json["featuregroups"])
    join_key = input_json["joinKey"]
    featurestore_query = input_json["featurestore"]
    training_dataset_name = input_json["trainingDataset"]
    training_dataset_data_format = input_json["dataFormat"]
    training_dataset_version = input_json["version"]
    descriptive_stats = input_json["descriptiveStats"]
    featurecorrelation = input_json["featureCorrelation"]
    clusteranalysis = input_json["clusterAnalysis"]
    featurehistograms = input_json["featureHistograms"]
    statcolumns = pre_process_stat_columns(input_json["statColumns"])
    sink = input_json["sink"] if "sink" in input_json else None
    path = input_json["path"] if "path" in input_json else None

    # Get Features
    features_df = featurestore.get_features(
        features,
        featurestore=featurestore_query,
        featuregroups_version_dict=featuregroups_version_dict,
        join_key = join_key,
        dataframe_type = "spark"
    )

    # Save as Training Dataset
    featurestore.create_training_dataset(
        features_df, training_dataset_name,
        description=training_dataset_desc,
        featurestore=featurestore_query,
        data_format=training_dataset_data_format,
        training_dataset_version=int(training_dataset_version),
        descriptive_statistics=descriptive_stats,
        feature_correlation=featurecorrelation,
        feature_histograms=clusteranalysis,
        cluster_analysis=featurehistograms,
        stat_columns=statcolumns,
        sink=sink,
        path=path
    )


def insert_into_training_dataset(input_json):
    """
    Creates a training dataset based on command-line arguments

    Args:
        :input_json: input JSON arguments

    Returns:
        None
    """

    features = pre_process_features(input_json["features"])
    featuregroups_version_dict = pre_process_featuregroups(input_json["featuregroups"])
    join_key = input_json["joinKey"]
    featurestore_query = input_json["featurestore"]
    training_dataset_name = input_json["trainingDataset"]
    training_dataset_desc = input_json["description"]
    training_dataset_data_format = input_json["dataFormat"]
    training_dataset_version = input_json["version"]
    descriptive_stats = input_json["descriptiveStats"]
    featurecorrelation = input_json["featureCorrelation"]
    clusteranalysis = input_json["clusterAnalysis"]
    featurehistograms = input_json["featureHistograms"]
    statcolumns = pre_process_stat_columns(input_json["statColumns"])
    sink = input_json["sink"] if "sink" in input_json else None
    path = input_json["path"] if "path" in input_json else None

    # Get Features
    features_df = featurestore.get_features(
        features,
        featurestore=featurestore_query,
        featuregroups_version_dict=featuregroups_version_dict,
        join_key = join_key,
        dataframe_type = "spark"
    )

    # Save as Training Dataset
    featurestore.insert_into_training_dataset(
        features_df, training_dataset_name,
        featurestore=featurestore_query,
        data_format=training_dataset_data_format,
        training_dataset_version=int(training_dataset_version),
        descriptive_statistics=descriptive_stats,
        feature_correlation=featurecorrelation,
        feature_histograms=clusteranalysis,
        cluster_analysis=featurehistograms,
        stat_columns=statcolumns,
    )



def main():
    """
    Program orchestrator
    """
    # Setup Spark
    spark, sc, sqlContext = setup_spark()

    # Setup Arguments
    args = parse_args()

    # Parse input JSON
    if args.input is None:
        raise ValueError("Input path to JSON file cannot be None")
    input_json = parse_input_json(args.input)

    # Perform Operation
    if input_json["operation"] == "create_td":
        create_training_dataset(input_json)
    elif input_json["operation"] == "insert_into_td":
        insert_into_training_dataset(input_json)
    else:
        raise ValueError("Unrecognized featurestore util py operation: {}. The Python util job only supports the "
                         "operation 'create_td', for other operations, "
                         "see the featurestoreutil4j job.".format(input_json["operation"]))

    #Cleanup
    spark.stop()

    return None


# Program entrypoint
if __name__ == '__main__':
    main()
