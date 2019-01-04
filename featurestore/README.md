# Example Feature Engineering Job for the Hops Feature Store

This job is used in the feature store "tour" on hopsworks to demonstrate the functionalities of the feature store.

This job was developed using Spark version 2.3.2 and scala version 2.11.x

It will output five feature groups according to this model:

![model](./model.png "Model")

## How to run

### Build

1. **Build jar**
   - `$ mvn clean install`

### Run

2. **Run as spark-job**
   - submit to cluster or run locally
