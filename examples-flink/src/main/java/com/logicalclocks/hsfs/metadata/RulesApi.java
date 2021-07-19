package com.logicalclocks.hsfs.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.metadata.validation.RuleName;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.logicalclocks.hsfs.metadata.HopsworksClient.API_PATH;
import static com.logicalclocks.hsfs.metadata.HopsworksClient.getInstance;

public class RulesApi {

  public static final String RULE_DEFINITIONS_PATH = API_PATH
      + "/rules{/name}{/predicate}{?filter_by,sort_by,offset,limit}";

  private static final Logger LOGGER = LoggerFactory.getLogger(RulesApi.class);


  public scala.collection.Seq<RuleDefinition> get() throws FeatureStoreException, IOException {
    return JavaConverters.asScalaBufferConverter(getRules(null)).asScala().toSeq();
  }

  public RuleDefinition get(RuleName name) throws FeatureStoreException, IOException {
    return getRules(name).get(0);
  }

  private List<RuleDefinition> getRules(RuleName name)
      throws FeatureStoreException, IOException {

    UriTemplate uriTemplate = UriTemplate.fromTemplate(RULE_DEFINITIONS_PATH);

    if (name != null) {
      uriTemplate.set("name", name);
    }
    String uri = uriTemplate.expand();

    LOGGER.info("Sending metadata request: " + uri);
    HttpGet getRequest = new HttpGet(uri);
    HopsworksClient hopsworksClient = getInstance();
    RuleDefinition rulesDto = hopsworksClient.handleRequest(getRequest, RuleDefinition.class);
    List<RuleDefinition> rules;
    if (rulesDto.getCount() == null) {
      rules = new ArrayList<>();
      rules.add(rulesDto);
    } else {
      rules = rulesDto.getItems();
    }
    LOGGER.info("Received ruleDefinitions: " + rules);
    return rules;
  }
}
