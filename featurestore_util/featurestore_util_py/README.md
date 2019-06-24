# Feature Store Util - Training Dataset Creation 

## `featurestore_create_training_dataset.py`

A utility program for launching spark jobs to do common operations in the Hopsworks Feature Store such as (1) 
creating training datasets; (2) computing feature statistics.

### Usage

Submit as a Spark Job on Hopsworks and configure it using the command line parameters. 

To create a training dataset called ```test_td_job``` with version ```1``` and format ```parquet``` consisting of the features 
```sum_player_age,sum_player_rating,sum_attendance,average_attendance``` from the feature groups 
```attendances_features,players_features``` with versions ```1,1``` use the following command line parameters:

```bash
--features "sum_player_age,sum_player_rating,sum_attendance,average_attendance" \
--featuregroups "attendances_features:1,players_features:1" \
--trainingdataset "test_td_job" \
--version "1" \
--dataformat "parquet" \
--joinkey "team_id" \
--description "test create_td job"
```

To update the feature statistics for a training dataset called ```test_java_td``` with version ```1``` use the 
following command line parameters:
```bash
--trainingdataset 'test_java_td' \ 
--operation 'update_td_stats'  \
--descriptivestats  \
--featurecorrelation \
--clusteranalysis \
--featurehistograms \
--version '1' \ 
--featurestore 'demo_featurestore_admin000_featurestore' \a
```

To update the feature statistics for a feature group called ```pandas_test_example``` with version ```1``` use the 
following command line parameters:
```bash
--featuregroup 'pandas_test_example' 
--operation 'update_fg_stats' 
--descriptivestats
--featurecorrelation 
--clusteranalysis 
--featurehistograms 
--version '1' 
--featurestore 
'demo_featurestore_admin000_featurestore' 
```

### Options

| Parameter-name                        | Description                                                                                                      |
| -----                                 | -----------                                                                                                      |
| -f --features                         | comma separated list of features                                                                                 |
| -fgs --featuregroups                  | comma separated list of featuregroups with the form `featuregroup:version` where the features reside             |
| -fs --featurestore                    | the featurestore to query                                                                                        |
| -td --trainingdataset                 | name of the training dataset                                                                                     |
| -td --featuregroup                    | name of the feature group                                                                                        |
| -jk  --joinkey                        | the key to join the features from the different featuregroups on                                                 |
| -desc --description                   | description                                                                                                      |
| -df --dataformat                      | format of the training dataset                                                                                   |
| -v --version                          | version                                                                                                          |
| -ds --descriptivestats                | boolean flag whether to compute the descriptive stats                                                            |
| -fc --featurecorrelation              | boolean flag whether to compute the feature correlations                                                         |
| -ca --clusteranalysis                 | boolean flag whether to compute the cluster analysis                                                             |
| -fh --featurehistograms               | boolean flag whether to compute the feature histograms                                                           |
| -sc --statcolumns                     | comma separated list of columns to apply statistics to (if empty use all columns)                                |
| -op --operation                       | the operation to compute (e.g 'create_td' or 'update_fg_stats'                                                   |

