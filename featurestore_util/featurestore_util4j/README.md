# Feature Store Util 

## `featurestore_util4j`

A utility program for launching spark jobs to do common operations in the Hopsworks Feature Store such as (1) 
creating training datasets; (2) computing feature statistics.

### Usage

Submit as a Spark Job on Hopsworks and configure it using the command line parameters.  

### Options

| Parameter-name                        | Description                                                                                                      |
| -----                                 | -----------                                                                                                      |
| -f --features                         | comma separated list of features                                                                                 |
| -fgs --featuregroups                  | comma separated list of featuregroups with the form `featuregroup:version` where the features reside             |
| -fs --featurestore                    | the featurestore to query                                                                                        |
| -td --trainingdataset                 | name of the training dataset                                                                                     |
| -fg --featuregroup                    | name of the feature group                                                  
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

