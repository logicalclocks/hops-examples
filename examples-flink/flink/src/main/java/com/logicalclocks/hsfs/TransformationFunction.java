package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.metadata.RestDto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransformationFunction extends RestDto<TransformationFunction> {
  @Getter
  @Setter
  private Integer id;
  @Getter
  @Setter
  private String name;
  @Getter
  @Setter
  private String outputType;
  @Getter
  @Setter
  private Integer version;
  @Getter
  @Setter
  private String sourceCodeContent;
  @Getter
  @Setter
  private Integer featurestoreId;
}
