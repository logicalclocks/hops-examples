package com.logicalclocks.flink.schemas;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString

public class AggregatedTransactions {
  @Getter
  @Setter
  @JsonProperty("cc_num")
  private Long ccNumber;

  @Getter
  @Setter
  @JsonProperty("num_trans_per_10m")
  private Long num_trans_per_10m;

  @Getter
  @Setter
  @JsonProperty("avg_amt_per_10m")
  private Double avg_amt_per_10m;

  @Getter
  @Setter
  @JsonProperty("stdev_amt_per_10m")
  private Double stdev_amt_per_10m;

  @Getter
  @Setter
  @JsonProperty("num_trans_per_1h")
  private Long num_trans_per_1h;

  @Getter
  @Setter
  @JsonProperty("avg_amt_per_1h")
  private Double avg_amt_per_1h;

  @Getter
  @Setter
  @JsonProperty("stdev_amt_per_1h")
  private Double stdev_amt_per_1h;

  @Getter
  @Setter
  @JsonProperty("num_trans_per_12h")
  private Long num_trans_per_12h;

  @Getter
  @Setter
  @JsonProperty("avg_amt_per_12h")
  private Double avg_amt_per_12h;

  @Getter
  @Setter
  @JsonProperty("stdev_amt_per_12h")
  private Double stdev_amt_per_12h;
}
