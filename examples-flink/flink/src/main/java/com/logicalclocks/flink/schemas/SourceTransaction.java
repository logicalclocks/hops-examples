package com.logicalclocks.flink.schemas;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
public class SourceTransaction {
  @Getter
  @Setter
  @JsonProperty("tid")
  private String tid;

  @Getter
  @Setter
  @JsonProperty("datetime")
  private String datetime;

  @Getter
  @Setter
  @JsonProperty("cc_num")
  private Long ccNumber;

  @Getter
  @Setter
  @JsonProperty("amount")
  private Double amount;
}
