"""
featurestore_util.py
~~~~~~~~~~

This Python module contains utility functions for starting
Apache Spark jobs for doing common operations in The Hopsworks Feature Store.
Such as, (1) creating a training dataset from a set of features. It will take the set of features and a join key as
input arguments, join the features together into a spark dataframe,
and write it out as a training dataset; (2) updating feature group or training dataset statistics.
"""

from hops import featurestore
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import argparse

def parse_args():
    """Parses the commandline arguments with argparse"""
    parser = argparse.ArgumentParser(description='Parse flags to configure the json parsing')
    parser.add_argument("-f", "--features", help="comma separated list of features",
                        default="")
    parser.add_argument("-fgs", "--featuregroups", help="comma separated list of featuregroups on the form "
                                                      "`featuregroup:version` where the features reside",
                        default="")
    parser.add_argument("-fs", "--featurestore", help="name of the feature store to apply the operation to",
                        default=featurestore.project_featurestore())
    parser.add_argument("-td", "--trainingdataset", help="name of the training dataset",
                        default="")
    parser.add_argument("-fg", "--featuregroup", help="name of the feature group",
                        default="")
    parser.add_argument("-jk", "--joinkey", help="join key for joining the features together",
                        default=None)
    parser.add_argument("-desc", "--description", help="description",
                        default="")
    parser.add_argument("-df", "--dataformat", help="format of the training dataset",
                        default="parquet")
    parser.add_argument("-v", "--version", help="version",
                        default="1")
    parser.add_argument("-ds", "--descriptivestats",
                        help="flag whether to compute descriptive stats",
                        action="store_true")
    parser.add_argument("-fc", "--featurecorrelation",
                        help="flag whether to compute feature correlations",
                        action="store_true")
    parser.add_argument("-ca", "--clusteranalysis",
                        help="flag whether to compute cluster analysis",
                        action="store_true")
    parser.add_argument("-fh", "--featurehistograms",
                        help="flag whether to compute feature histograms",
                        action="store_true")
    parser.add_argument("-sc", "--statcolumns",
                        help="comma separated list of columns to apply statisics to (if empty use all columns)",
                        default="")
    parser.add_argument("-op", "--operation",
                        help="the feature store operation",
                        default="create_td")
    args = parser.parse_args()
    return args

def setup_spark():
    """
    Setup the spark session

    :return: sparksession, sparkContext, sqlContext
    """
    spark=SparkSession.builder.enableHiveSupport().getOrCreate()
    sc=spark.sparkContext
    sqlContext=SQLContext(sc)
    return spark, sc, sqlContext

def pre_process_features(features_str):
    """
    Pre process the string passed on command-line

    :param features_str: the strsing passed on command-line
    :return: a list of features
    """
    if(features_str == ''):
        raise ValueError("features cannot be empty")
    return features_str.split(",")

def pre_process_stat_columns(statcolumns_str):
    """
    Pre process the command-line input into a list

    :param statcolumns_str: the string passed on command-line
    :return: a list of stat columns or None
    """
    if(statcolumns_str == ''):
        return None
    else:
        return statcolumns_str.split(",")

def pre_process_featuregroups(featuregroups_versions_str):
    """
    Pre process the command-line input into a dict with fg-->version

    :param featuregroups_versions_str: the string passed on command-line
    :return: a dict with fg-->version
    """
    if(featuregroups_versions_str == ''):
        raise ValueError("featuregroups cannot be empty")
    featuregroups_versions = featuregroups_versions_str.split(",")
    featuregroups_version_dict = {}
    for fg_v in featuregroups_versions:
        fg_v_list = fg_v.split(":")
        featuregroup = fg_v_list[0]
        version = fg_v_list[1]
        featuregroups_version_dict[featuregroup] = version
    return featuregroups_version_dict

def create_training_dataset(args):
    """
    Creates a training dataset based on command-line arguments

    :param args: input arguments
    :return: None
    """
    features = pre_process_features(args.features)
    featuregroups_version_dict = pre_process_featuregroups(args.featuregroups)
    join_key = args.joinkey
    featurestore_query = args.featurestore
    training_dataset_name = args.trainingdataset
    training_dataset_desc = args.description
    training_dataset_data_format = args.dataformat
    training_dataset_version = args.version
    descriptive_stats = args.descriptivestats
    featurecorrelation = args.featurecorrelation
    clusteranalysis = args.clusteranalysis
    featurehistograms = args.featurehistograms
    statcolumns = pre_process_stat_columns(args.statcolumns)

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
    )

def update_fg_stats(args):
    """
    Update feature group statistics based on command-line arguments

    :param args: command-line arguments
    :return: None
    """
    featuregroup = args.featuregroup
    version = args.version
    descriptive_stats = args.descriptivestats
    featurecorrelation = args.featurecorrelation
    clusteranalysis = args.clusteranalysis
    featurehistograms = args.featurehistograms
    featurestore_query = args.featurestore
    statcolumns = pre_process_stat_columns(args.statcolumns)
    featurestore.update_featuregroup_stats(featuregroup, featuregroup_version=version,
                                           featurestore=featurestore_query, descriptive_statistics=descriptive_stats,
                                           feature_correlation=featurecorrelation, feature_histograms=featurehistograms,
                                           cluster_analysis=clusteranalysis, stat_columns=statcolumns)


def update_td_stats(args):
    """
    Update training dataset statistics based on command-line arguments

    :param args: command-line arguments
    :return: None
    """
    training_dataset = args.trainingdataset
    version = args.version
    descriptive_stats = args.descriptivestats
    featurecorrelation = args.featurecorrelation
    clusteranalysis = args.clusteranalysis
    featurehistograms = args.featurehistograms
    featurestore_query = args.featurestore
    statcolumns = pre_process_stat_columns(args.statcolumns)
    featurestore.update_training_dataset_stats(training_dataset, training_dataset_version=version,
                                           featurestore=featurestore_query, descriptive_statistics=descriptive_stats,
                                           feature_correlation=featurecorrelation, feature_histograms=featurehistograms,
                                           cluster_analysis=clusteranalysis, stat_columns=statcolumns)

def main():
    """
    Program orchestrator
    """
    # Setup Spark
    spark, sc, sqlContext = setup_spark()

    # Setup Arguments
    args = parse_args()

    # Perform Operation
    if args.operation == "create_td":
        create_training_dataset(args)

    if args.operation == "update_fg_stats":
        update_fg_stats(args)

    if args.operation == "update_td_stats":
        update_td_stats(args)

    #Cleanup
    spark.close()

    return None


# Program entrypoint
if __name__ == '__main__':
    main()
