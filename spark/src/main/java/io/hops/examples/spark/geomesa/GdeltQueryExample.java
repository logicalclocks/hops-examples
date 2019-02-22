package io.hops.examples.spark.geomesa;

import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.factory.Hints;
import org.geotools.filter.identity.FeatureIdImpl;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.fs.FileSystemDataStoreFactory;
import org.locationtech.geomesa.fs.storage.interop.PartitionSchemeUtils;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GdeltQueryExample {
  
  private static Logger LOG = Logger.getLogger("GdeltQueryExample");
  
  
  private final Map<String, String> params;
  private final TutorialData data;
  private final boolean cleanup;
  private final boolean readOnly;
  
  public GdeltQueryExample(String[] args, DataAccessFactory.Param[] parameters, TutorialData data) throws ParseException {
    this(args, parameters, data, false);
  }
  
  public GdeltQueryExample(String[] args, DataAccessFactory.Param[] parameters, TutorialData data, boolean readOnly) throws ParseException {
    // parse the data store parameters from the command line
    Properties options = createOptions(parameters);
    Properties commands = CommandLineDataStore.parseArgs(getClass(), options, args);
    params = CommandLineDataStore.getDataStoreParams(commands);
    cleanup = commands.contains("cleanup");
    this.data = data;
    this.readOnly = readOnly;
  }
  
  public Properties createOptions(DataAccessFactory.Param[] parameters) {
    // parse the data store parameters from the command line
    Properties options = CommandLineDataStore.createOptions(parameters);
    if (!readOnly) {
      options.setProperty("cleanup", "Delete tables after running");
    }
    return options;
  }
  
  public SimpleFeatureType getSimpleFeatureType(TutorialData data) {
  
  
    LOG.info("Get SimpleFeature Type");
    
    SimpleFeatureType sft = data.getSimpleFeatureType();
    // For the FSDS we need to modify the SimpleFeatureType to specify the index scheme
    org.locationtech.geomesa.fs.storage.api.PartitionScheme scheme = PartitionSchemeUtils.apply(sft, "daily,z2-2bit", Collections.emptyMap());
    PartitionSchemeUtils.addToSft(sft, scheme);
    
    System.out.println(sft.getTypes());
    return sft;
  }
  
  public DataStore createDataStore(Map<String, String> params) throws IOException {
    LOG.info("Loading datastore");
    LOG.info(params.toString());
    // use geotools service loading to get a datastore instance
    DataStore datastore = DataStoreFinder.getDataStore(params);
    if (datastore == null) {
      throw new RuntimeException("Could not create data store with provided parameters");
    }
    return datastore;
  }
  
  public void cleanup(DataStore datastore, String typeName, boolean cleanup) {
    if (datastore != null) {
      try {
        if (cleanup) {
          LOG.info("Cleaning up test data");
          if (datastore instanceof GeoMesaDataStore) {
            ((GeoMesaDataStore) datastore).delete();
          } else {
            ((SimpleFeatureStore) datastore.getFeatureSource(typeName)).removeFeatures(Filter.INCLUDE);
            datastore.removeSchema(typeName);
          }
        }
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Exception cleaning up test data: " + e.toString());
      } finally {
        // make sure that we dispose of the datastore when we're done with it
        datastore.dispose();
      }
    }
  }
  
  public void ensureSchema(DataStore datastore, TutorialData data) throws IOException {
    SimpleFeatureType sft = datastore.getSchema(data.getTypeName());
    if (sft == null) {
      throw new IllegalStateException("Schema '" + data.getTypeName() + "' does not exist. " +
        "Please run the associated QuickStart to generate the test data.");
    }
  }
  
  public void createSchema(DataStore datastore, SimpleFeatureType sft) throws IOException {
    System.out.println("Creating schema: " + DataUtilities.encodeType(sft));
    // we only need to do the once - however, calling it repeatedly is a no-op
    datastore.createSchema(sft);
    System.out.println();
  }
  
  public List<SimpleFeature> getTestFeatures(TutorialData data) {
    LOG.info("Generating test data");
    List<SimpleFeature> features = data.getTestData();
    return features;
  }
  
  public List<Query> getTestQueries(TutorialData data) {
    return data.getTestQueries();
  }
  
  public void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
    if (features.size() > 0) {
      LOG.info("Writing test data");
      
      // use try-with-resources to ensure the writer is closed
      
      //            ParquetFileSystemStorage.ParquetFileSystemWriter parquetFileSystemWriter = new ParquetFileSystemStorage.ParquetFileSystemWriter(sft, path, conf);
      //            parquetFileSystemWriter.write();
      
      try (FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
             datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT)) {
        
        for (SimpleFeature feature : features) {
          
          // using a geotools writer, you have to get a feature, modify it, then commit it
          // appending writers will always return 'false' for haveNext, so we don't need to bother checking
          SimpleFeature toWrite = writer.next();
          
          // copy attributes
          toWrite.setAttributes(feature.getAttributes());
          
          // if you want to set the feature ID, you have to cast to an implementation class
          // and add the USE_PROVIDED_FID hint to the user data
          ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
          toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
          
          // alternatively, you can use the PROVIDED_FID hint directly
          // toWrite.getUserData().put(Hints.PROVIDED_FID, feature.getID());
          
          // if no feature ID is set, a UUID will be generated for you
          
          // make sure to copy the user data, if there is any
          toWrite.getUserData().putAll(feature.getUserData());
          
          // write the feature
          //                    System.out.println(toWrite.getName());
          //                    System.out.println(toWrite.getProperties());
          //                    System.out.println(toWrite.getDefaultGeometry());
          //                    System.out.println(toWrite.getUserData());
          //                    System.out.println(toWrite.getAttributes());
          
          writer.write();
        }
      }
      LOG.info("Wrote " + features.size() + " features");
    }
  }
  
  public void queryFeatures(DataStore datastore, List<Query> queries) throws IOException {
    for (Query query : queries) {
      LOG.info("Running query " + ECQL.toCQL(query.getFilter()));
      if (query.getPropertyNames() != null) {
        LOG.info("Returning attributes " + Arrays.asList(query.getPropertyNames()));
      }
      if (query.getSortBy() != null) {
        SortBy sort = query.getSortBy()[0];
        LOG.info("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
      }
      // submit the query, and get back an iterator over matching features
      // use try-with-resources to ensure the reader is closed
      try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
             datastore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
        // loop through all results, only print out the first 10
        int n = 0;
        while (reader.hasNext()) {
          SimpleFeature feature = reader.next();
          if (n++ < 10) {
            // use geotools data utilities to get a printable string
            LOG.info(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
          } else if (n == 10) {
            LOG.info("...");
          }
        }
        LOG.info("Returned " + n + " total features");

      }
    }
  }
  
  public static void main(String[] args) throws IOException, ParseException {
    
    GdeltQueryExample gdeltQueryExample = new GdeltQueryExample(args, new FileSystemDataStoreFactory().getParametersInfo(), new GDELTData());
    
    DataStore datastore = null;
    try {
      
      
      SparkSession spark = SparkSession
        .builder()
        .appName("GdeltQueryExample")
        .getOrCreate();
      
      SparkContext sc = spark.sparkContext();
      
      datastore = gdeltQueryExample.createDataStore(gdeltQueryExample.params);
      
      if (gdeltQueryExample.readOnly) {
        gdeltQueryExample.ensureSchema(datastore, gdeltQueryExample.data);
      } else {
        SimpleFeatureType sft = gdeltQueryExample.getSimpleFeatureType(gdeltQueryExample.data);
  
        LOG.info(datastore.getClass().toString());
        gdeltQueryExample.createSchema(datastore, sft);
        List<SimpleFeature> features = gdeltQueryExample.getTestFeatures(gdeltQueryExample.data);
        gdeltQueryExample.writeFeatures(datastore, sft, features);
      }
      
      List<Query> queries = gdeltQueryExample.getTestQueries(gdeltQueryExample.data);
      
      gdeltQueryExample.queryFeatures(datastore, queries);
      
      
    } catch (Exception e) {
      throw new RuntimeException("Error running quickstart:", e);
    } finally {
      gdeltQueryExample.cleanup(datastore, gdeltQueryExample.data.getTypeName(), gdeltQueryExample.cleanup);
    }
    LOG.info("Done");
    
  }
  
  
  public Map<String, String> getParams() {
    return params;
  }
  
  public TutorialData getData() {
    return data;
  }
  
  public boolean isCleanup() {
    return cleanup;
  }
  
  public boolean isReadOnly() {
    return readOnly;
  }
}
