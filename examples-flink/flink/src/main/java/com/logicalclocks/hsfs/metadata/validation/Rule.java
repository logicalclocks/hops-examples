package com.logicalclocks.hsfs.metadata.validation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.metadata.RuleDefinition;
import com.logicalclocks.hsfs.metadata.RulesApi;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import scala.collection.JavaConverters;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@ToString
/*
 Used when creating rules for FeatureStore expectations.
 */
public class Rule {

  private final RulesApi featureStoreRulesApi = new RulesApi();

  @Getter @Setter
  private RuleName name;
  @Getter @Setter
  private Level level;
  @Getter @Setter
  private Double min;
  @Getter @Setter
  private Double max;
  @Getter @Setter
  private String value;
  @Getter @Setter
  private String pattern;
  @Getter @Setter
  private AcceptedType acceptedType;
  @Getter @Setter
  private List<String> legalValues;

  public static Rule.RuleBuilder createRule(RuleDefinition rule) {
    return com.logicalclocks.hsfs.metadata.validation.Rule.builder().rule(rule);
  }

  public static Rule.RuleBuilder createRule(RuleName name) {
    return com.logicalclocks.hsfs.metadata.validation.Rule.builder().name(name);
  }

  @Builder
  public Rule(RuleName name, RuleDefinition rule, Level level, Double min, Double max, String pattern,
      AcceptedType acceptedType, scala.collection.Seq<String> legalValues) {
    if (rule != null) {
      this.name = rule.getName();
    } else {
      this.name = name;
    }
    this.level = level;
    this.min = min;
    this.max = max;
    this.pattern = pattern;
    this.acceptedType = acceptedType;
    if (legalValues != null) {
      this.legalValues = (List<String>) JavaConverters.seqAsJavaListConverter(legalValues).asJava();
    }
  }

}
