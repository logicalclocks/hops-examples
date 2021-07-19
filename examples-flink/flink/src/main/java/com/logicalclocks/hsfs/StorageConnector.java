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

package com.logicalclocks.hsfs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import com.logicalclocks.hsfs.metadata.Option;
import com.logicalclocks.hsfs.metadata.StorageConnectorApi;
import com.logicalclocks.hsfs.util.Constants;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
/*
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
 */

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@NoArgsConstructor
@ToString
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "storageConnectorType", visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = StorageConnector.HopsFsConnector.class, name = "HOPSFS"),
  @JsonSubTypes.Type(value = StorageConnector.S3Connector.class, name = "S3"),
  @JsonSubTypes.Type(value = StorageConnector.RedshiftConnector.class, name = "REDSHIFT"),
  @JsonSubTypes.Type(value = StorageConnector.AdlsConnector.class, name = "ADLS"),
  @JsonSubTypes.Type(value = StorageConnector.SnowflakeConnector.class, name = "SNOWFLAKE"),
  @JsonSubTypes.Type(value = StorageConnector.JdbcConnector.class, name = "JDBC")
  })

public abstract class StorageConnector {

  @Getter @Setter
  protected StorageConnectorType storageConnectorType;

  @Getter @Setter
  private Integer id;

  @Getter @Setter
  private String name;

  @Getter @Setter
  private String description;

  @Getter @Setter
  private Integer featurestoreId;

  protected StorageConnectorApi storageConnectorApi = new StorageConnectorApi();

  /*
  public Dataset<Row> read(String query, String dataFormat, Map<String, String> options, String path)
      throws FeatureStoreException, IOException {
    return SparkEngine.getInstance().read(this, dataFormat, options, path);
  }
  */

  @JsonIgnore
  public abstract String getPath(String subPath) throws FeatureStoreException;

  public abstract Map<String, String> sparkOptions();

  public static class HopsFsConnector extends StorageConnector {

    @Getter @Setter
    private String hopsfsPath;

    @Getter @Setter
    private String datasetName;

    public Map<String, String> sparkOptions() {
      return new HashMap<>();
    }

    @JsonIgnore
    public String getPath(String subPath) {
      return hopsfsPath + "/" + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }
  }

  public static class S3Connector extends StorageConnector {

    @Getter @Setter
    private String accessKey;

    @Getter @Setter
    private String secretKey;

    @Getter @Setter
    private String serverEncryptionAlgorithm;

    @Getter @Setter
    private String serverEncryptionKey;

    @Getter @Setter
    private String bucket;

    @Getter @Setter
    private String sessionToken;

    @Getter @Setter
    private String iamRole;

    @JsonIgnore
    public String getPath(String subPath) {
      return "s3://" + bucket + "/"  + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }

    public Map<String, String> sparkOptions() {
      return new HashMap<>();
    }

    /*
    public Dataset<Row> read(String query, String dataFormat, Map<String, String> options, String path)
        throws FeatureStoreException, IOException {
      refetch();
      return SparkEngine.getInstance().read(this, dataFormat, options, path);
    }
    */

    public void refetch() throws FeatureStoreException, IOException {
      S3Connector updatedConnector = (S3Connector) storageConnectorApi.get(getFeaturestoreId(), getName());
      this.accessKey = updatedConnector.getAccessKey();
      this.secretKey = updatedConnector.getSecretKey();
      this.sessionToken = updatedConnector.getSessionToken();
    }
  }

  public static class RedshiftConnector extends StorageConnector {

    @Getter @Setter
    private String clusterIdentifier;

    @Getter @Setter
    private String databaseDriver;

    @Getter @Setter
    private String databaseEndpoint;

    @Getter @Setter
    private String databaseName;

    @Getter @Setter
    private Integer databasePort;

    @Getter @Setter
    private String tableName;

    @Getter @Setter
    private String databaseUserName;

    @Getter @Setter
    private Boolean autoCreate;

    @Getter @Setter
    private String databasePassword;

    @Getter @Setter
    private String databaseGroup;

    @Getter @Setter
    private String iamRole;

    @Getter @Setter
    private String arguments;

    @Getter @Setter
    private Instant expiration;

    public Map<String, String> sparkOptions() {
      String constr =
          "jdbc:redshift://" + clusterIdentifier + "." + databaseEndpoint + ":" + databasePort + "/" + databaseName;
      if (!Strings.isNullOrEmpty(arguments)) {
        constr += "?" + arguments;
      }
      Map<String, String> options = new HashMap<>();
      options.put(Constants.JDBC_DRIVER, databaseDriver);
      options.put(Constants.JDBC_URL, constr);
      options.put(Constants.JDBC_USER, databaseUserName);
      options.put(Constants.JDBC_PWD, databasePassword);
      if (!Strings.isNullOrEmpty(tableName)) {
        options.put(Constants.JDBC_TABLE, tableName);
      }
      return options;
    }

    /*
    public Dataset<Row> read(String query, String dataFormat, Map<String, String> options, String path)
        throws FeatureStoreException, IOException {
      refetch();
      Map<String, String> readOptions = sparkOptions();
      if (!Strings.isNullOrEmpty(query)) {
        readOptions.put("query", query);
      }
      return SparkEngine.getInstance().read(this, Constants.JDBC_FORMAT, readOptions, null);
    }
    */

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }

    public void refetch() throws FeatureStoreException, IOException {
      RedshiftConnector updatedConnector = (RedshiftConnector) storageConnectorApi.get(getFeaturestoreId(), getName());
      this.databaseUserName = updatedConnector.getDatabaseUserName();
      this.expiration = updatedConnector.getExpiration();
      this.databasePassword = updatedConnector.getDatabasePassword();
    }
  }

  public static class AdlsConnector extends StorageConnector {

    @Getter @Setter
    private Integer generation;

    @Getter @Setter
    private String directoryId;

    @Getter @Setter
    private String applicationId;

    @Getter @Setter
    private String serviceCredential;

    @Getter @Setter
    private String accountName;

    @Getter @Setter
    private String containerName;

    @Getter @Setter
    private List<Option> sparkOptions;

    @JsonIgnore
    public String getPath(String subPath) {
      return (this.generation == 2
          ? "abfss://" + this.containerName + "@" + this.accountName + ".dfs.core.windows.net/"
          : "adl://" + this.accountName + ".azuredatalakestore.net/")
          + (Strings.isNullOrEmpty(subPath) ? "" : subPath);
    }

    public Map<String, String> sparkOptions() {
      Map<String, String> options = new HashMap<>();
      sparkOptions.stream().forEach(option -> options.put(option.getName(), option.getValue()));
      return options;
    }
  }

  public static class SnowflakeConnector extends StorageConnector {

    @Getter @Setter
    private String url;

    @Getter @Setter
    private String user;

    @Getter @Setter
    private String password;

    @Getter @Setter
    private String token;

    @Getter @Setter
    private String database;

    @Getter @Setter
    private String schema;

    @Getter @Setter
    private String warehouse;

    @Getter @Setter
    private String role;

    @Getter @Setter
    private String table;

    @Getter @Setter
    private List<Option> sfOptions;

    public String account() {
      return this.url.replace("https://", "").replace(".snowflakecomputing.com", "");
    }

    public Map<String, String> sparkOptions() {
      Map<String, String> options = new HashMap<>();
      options.put(Constants.SNOWFLAKE_URL, url);
      options.put(Constants.SNOWFLAKE_SCHEMA, schema);
      options.put(Constants.SNOWFLAKE_DB, database);
      options.put(Constants.SNOWFLAKE_USER, user);
      if (!Strings.isNullOrEmpty(password)) {
        options.put(Constants.SNOWFLAKE_PWD, password);
      } else {
        options.put(Constants.SNOWFLAKE_AUTH, "oauth");
        options.put(Constants.SNOWFLAKE_TOKEN, token);
      }
      if (!Strings.isNullOrEmpty(warehouse)) {
        options.put(Constants.SNOWFLAKE_WAREHOUSE, warehouse);
      }
      if (!Strings.isNullOrEmpty(role)) {
        options.put(Constants.SNOWFLAKE_ROLE, role);
      }
      if (!Strings.isNullOrEmpty(table)) {
        options.put(Constants.SNOWFLAKE_TABLE, table);
      }
      if (sfOptions != null && !sfOptions.isEmpty()) {
        Map<String, String> argOptions = sfOptions.stream()
            .collect(Collectors.toMap(Option::getName, Option::getValue));
        options.putAll(argOptions);
      }
      return options;
    }

    /*
    public Dataset<Row> read(String query, String dataFormat, Map<String, String> options, String path) {
      Map<String, String> readOptions = sparkOptions();
      if (!Strings.isNullOrEmpty(query)) {
        readOptions.put("query", query);
      }
      return SparkEngine.getInstance().read(this, Constants.SNOWFLAKE_FORMAT, readOptions, null);
    }
    */

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }
  }

  public static class JdbcConnector extends StorageConnector {

    @Getter @Setter
    private String connectionString;

    @Getter @Setter
    private String arguments;

    public Map<String, String> sparkOptions() {
      Map<String, String> options = Arrays.stream(arguments.split(","))
          .map(arg -> arg.split("="))
          .collect(Collectors.toMap(a -> a[0], a -> a[1]));
      options.put(Constants.JDBC_URL, connectionString);
      return options;
    }

    /*
    public Dataset<Row> read(String query, String dataFormat, Map<String, String> options, String path) {
      Map<String, String> readOptions = sparkOptions();
      if (!Strings.isNullOrEmpty(query)) {
        readOptions.put("query", query);
      }
      return SparkEngine.getInstance().read(this, Constants.JDBC_FORMAT, readOptions, null);
    }
    */

    @JsonIgnore
    public String getPath(String subPath) {
      return null;
    }
  }
}
