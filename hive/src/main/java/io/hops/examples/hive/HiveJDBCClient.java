/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.examples.hive;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Demo class showing how to connect to Hive on Hops using a secure JDBC connection.
 * It initiates a secure connection to hive server and executes a query. The database and query used are
 * taken from Hops documentation. More detailed documentation on using JDBC with HIVE is available here:
 * https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBCClientSampleCode
 * <p>
 */
public class HiveJDBCClient {
  
  private static final Logger LOG = LogManager.getLogger(HiveJDBCClient.class);
  private static final String HIVE_CREDENTIALS = "hive_credentials.properties";
  
  //Hive credentials property names
  private static final String HIVE_URL = "hive_url";
  private static final String DB_NAME = "dbname";
  private static final String TRUSTSTORE_PATH = "truststore_path";
  private static final String TRUSTSTORE_PW = "truststore_pw";
  private static final String KEYSTORE_PATH = "keystore_path";
  private static final String KEYSTORE_PW = "keystore_pw";
  private static final String HOPSWORKS_USERNAME = "hopsworks_username";
  private static final String HOPSWORKS_PW = "hopsworks_pw";
  
  public static void main(String[] args) throws SQLException, IOException {
    
    Connection conn = HiveJDBCClient.getHiveJDBCConnection();
    //Example of setting hive/tez properties
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict;");
    }
    
    
    
    PreparedStatement prepStatement = conn.prepareStatement("insert overwrite table orc_table select * from sales");
    prepStatement.execute();
    
    //    try (PreparedStatement prepStatement = conn.prepareStatement(
    //        "select city, avg(price) as price from sales_orc group by city")) {
    //      try (ResultSet rst = prepStatement.executeQuery()) {
    //        LOG.info("City \t Price");
    //        while (rst.next()) {
    //          LOG.info(rst.getString(1) + "\t" + rst.getString(2));
    //        }
    //      }
    //    }
    
    
    
    LOG.info("Exiting...");
    
  }
  
  /**
   * Initializes a JDBC connection to Hopsworks Hive server by reading credentials from properties file.
   *
   * @return
   * @throws SQLException
   * @throws IOException
   */
  private static Connection getHiveJDBCConnection() throws SQLException, IOException {
    LOG.info("Reading hive credentials from properties file");
    Properties hiveCredentials = readHiveCredentials(HIVE_CREDENTIALS);
    LOG.info("Establishing connection to Hive server at:" + hiveCredentials.getProperty(HIVE_URL));
    Connection conn = DriverManager.getConnection(hiveCredentials.getProperty(HIVE_URL) + "/"
        + hiveCredentials.getProperty(DB_NAME) + ";auth=noSasl;ssl=true;twoWay=true;sslTrustStore="
        + hiveCredentials.getProperty(TRUSTSTORE_PATH) + ";trustStorePassword="
        + hiveCredentials.getProperty(TRUSTSTORE_PW)+ ";sslKeyStore="
        + hiveCredentials.getProperty(KEYSTORE_PATH) + ";keyStorePassword="
        + hiveCredentials.getProperty(KEYSTORE_PW)
      //      hiveCredentials.getProperty(HOPSWORKS_USERNAME),
      //      hiveCredentials.getProperty(HOPSWORKS_PW)
    );
    LOG.info("Connection established!");
    return conn;
  }
  
  private static Properties readHiveCredentials(String path) throws IOException {
    InputStream stream = HiveJDBCClient.class.getClassLoader().getResourceAsStream("./io/hops/examples/" + path);
    if (stream == null) {
      throw new IOException("No ." + HIVE_CREDENTIALS + " properties file found");
    }
    Properties props = new Properties();
    props.load(stream);
    return props;
  }
  
}
