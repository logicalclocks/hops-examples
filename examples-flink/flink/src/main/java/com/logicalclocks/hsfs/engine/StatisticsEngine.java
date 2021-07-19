/*
 * Copyright (c) 2020 Logical Clocks AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.logicalclocks.hsfs.engine;

import com.logicalclocks.hsfs.EntityEndpointType;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.TrainingDataset;
import com.logicalclocks.hsfs.metadata.FeatureGroupBase;
import com.logicalclocks.hsfs.metadata.Statistics;
import com.logicalclocks.hsfs.metadata.StatisticsApi;
/*
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
*/
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StatisticsEngine {

  private StatisticsApi statisticsApi;

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsEngine.class);

  public StatisticsEngine(EntityEndpointType entityType) {
    this.statisticsApi = new StatisticsApi(entityType);
  }

  /*
  public Statistics computeStatistics(TrainingDataset trainingDataset, Dataset<Row> dataFrame)
      throws FeatureStoreException, IOException {
    return statisticsApi.post(trainingDataset, computeStatistics(dataFrame,
        trainingDataset.getStatisticsConfig().getColumns(),
        trainingDataset.getStatisticsConfig().getHistograms(),
        trainingDataset.getStatisticsConfig().getCorrelations(),
        null));
  }

  public Statistics computeStatistics(FeatureGroupBase featureGroup, Dataset<Row> dataFrame, Long commitId)
      throws FeatureStoreException, IOException {
    return statisticsApi.post(featureGroup, computeStatistics(dataFrame,
        featureGroup.getStatisticsConfig().getColumns(),
        featureGroup.getStatisticsConfig().getHistograms(),
        featureGroup.getStatisticsConfig().getCorrelations(),
        commitId));
  }

  private Statistics computeStatistics(Dataset<Row> dataFrame, List<String> statisticColumns, Boolean histograms,
                                       Boolean correlations, Long commitId) throws FeatureStoreException {
    if (dataFrame.isEmpty()) {
      throw new FeatureStoreException("There is no data in the entity that you are trying to compute statistics for. A "
          + "possible cause might be that you inserted only data to the online storage of a feature group.");
    }
    Long commitTime = Timestamp.valueOf(LocalDateTime.now()).getTime();
    String content = SparkEngine.getInstance().profile(dataFrame, statisticColumns, histograms, correlations);
    return new Statistics(commitTime, commitId, content);
  }
   */

  public Statistics get(FeatureGroupBase featureGroup, String commitTime) throws FeatureStoreException, IOException {
    return statisticsApi.get(featureGroup, commitTime);
  }

  public Statistics get(TrainingDataset trainingDataset, String commitTime) throws FeatureStoreException, IOException {
    return statisticsApi.get(trainingDataset, commitTime);
  }

  public Statistics getLast(FeatureGroupBase featureGroup) throws FeatureStoreException, IOException {
    return statisticsApi.getLast(featureGroup);
  }

  public Statistics getLast(TrainingDataset trainingDataset) throws FeatureStoreException, IOException {
    return statisticsApi.getLast(trainingDataset);
  }
}
