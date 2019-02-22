/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package io.hops.examples.spark.geomesa;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class GDELTData implements TutorialData {
  
  private static final Logger logger = LoggerFactory.getLogger(GDELTData.class);
  
  private SimpleFeatureType sft = null;
  private List<SimpleFeature> features = null;
  private List<Query> queries = null;
  private Filter subsetFilter = null;
  
  public String getTypeName() {
    return "gdelt-quickstart";
  }
  
  @Override
  public SimpleFeatureType getSimpleFeatureType() {
    if (sft == null) {
      // list the attributes that constitute the feature type
      // this is a reduced set of the attributes from GDELT 2.0
      StringBuilder attributes = new StringBuilder();
      attributes.append("GLOBALEVENTID:String,");
      attributes.append("Actor1Name:String,");
      attributes.append("Actor1CountryCode:String,");
      attributes.append("Actor2Name:String,");
      attributes.append("Actor2CountryCode:String,");
      attributes.append("EventCode:String:index=true,"); // marks this attribute for indexing
      attributes.append("NumMentions:Integer,");
      attributes.append("NumSources:Integer,");
      attributes.append("NumArticles:Integer,");
      attributes.append("ActionGeo_Type:Integer,");
      attributes.append("ActionGeo_FullName:String,");
      attributes.append("ActionGeo_CountryCode:String,");
      attributes.append("dtg:Date,");
      attributes.append("*geom:Point:srid=4326"); // the "*" denotes the default geometry (used for indexing)
      
      // create the simple-feature type - use the GeoMesa 'SimpleFeatureTypes' class for best compatibility
      // may also use geotools DataUtilities or SimpleFeatureTypeBuilder, but some features may not work
      sft = SimpleFeatureTypes.createType(getTypeName(), attributes.toString());
      
      // use the user-data (hints) to specify which date field to use for primary indexing
      // if not specified, the first date attribute (if any) will be used
      // could also use ':default=true' in the attribute specification string
      sft.getUserData().put(SimpleFeatureTypes.Configs.DEFAULT_DATE_KEY(), "dtg");
    }
    return sft;
  }
  
  @Override
  public List<SimpleFeature> getTestData() {
    if (features == null) {
      List<SimpleFeature> features = new ArrayList<>();
      
      // read the bundled GDELT 2.0 TSV
      URL input = getClass().getClassLoader().getResource("20180101000000.export.CSV");
      if (input == null) {
        throw new RuntimeException("Couldn't load resource 20180101000000.export.CSV");
      }
      
      // date parser corresponding to the CSV format
      DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.US);
      
      // use a geotools SimpleFeatureBuilder to create our features
      SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());
      
      // use apache commons-csv to parse the GDELT file
      try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.TDF)) {
        for (CSVRecord record : parser) {
          try {
            // pull out the fields corresponding to our simple feature attributes
            builder.set("GLOBALEVENTID", record.get(0));
            
            // some dates are converted implicitly, so we can set them as strings
            // however, the date format here isn't one that is converted, so we parse it into a java.util.Date
            builder.set("dtg",
              Date.from(LocalDate.parse(record.get(1), dateFormat).atStartOfDay(ZoneOffset.UTC).toInstant()));
            
            builder.set("Actor1Name", record.get(6));
            builder.set("Actor1CountryCode", record.get(7));
            builder.set("Actor2Name", record.get(16));
            builder.set("Actor2CountryCode", record.get(17));
            builder.set("EventCode", record.get(26));
            
            // we can also explicitly convert to the appropriate type
            builder.set("NumMentions", Integer.valueOf(record.get(31)));
            builder.set("NumSources", Integer.valueOf(record.get(32)));
            builder.set("NumArticles", Integer.valueOf(record.get(33)));
            
            builder.set("ActionGeo_Type", record.get(51));
            builder.set("ActionGeo_FullName", record.get(52));
            builder.set("ActionGeo_CountryCode", record.get(53));
            
            // we can use WKT (well-known-text) to represent geometries
            // note that we use longitude first ordering
            double latitude = Double.parseDouble(record.get(56));
            double longitude = Double.parseDouble(record.get(57));
            builder.set("geom", "POINT (" + longitude + " " + latitude + ")");
            
            // be sure to tell GeoTools explicitly that we want to use the ID we provided
            builder.featureUserData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);
            
            // build the feature - this also resets the feature builder for the next entry
            // use the GLOBALEVENTID as the feature ID
            SimpleFeature feature = builder.buildFeature(record.get(0));
            
            features.add(feature);
          } catch (Exception e) {
            logger.debug("Invalid GDELT record: " + e.toString() + " " + record.toString());
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Error reading GDELT data:", e);
      }
      this.features = Collections.unmodifiableList(features);
    }
    return features;
  }
  
  @Override
  public List<Query> getTestQueries() {
    if (queries == null) {
      try {
        List<Query> queries = new ArrayList<>();
        
        // most of the data is from 2018-01-01
        // note: DURING is endpoint exclusive
        String during = "dtg DURING 2017-12-31T00:00:00.000Z/2018-01-02T00:00:00.000Z";
        // bounding box over most of the united states
        String bbox = "bbox(geom,-120,30,-75,55)";
        
        // basic spatio-temporal query
        queries.add(new Query(getTypeName(), ECQL.toFilter(bbox + " AND " + during)));
        // basic spatio-temporal query with projection down to a few attributes
        queries.add(new Query(getTypeName(), ECQL.toFilter(bbox + " AND " + during),
          new String[]{ "GLOBALEVENTID", "dtg", "geom" }));
        // attribute query on a secondary index - note we specified index=true for EventCode
        queries.add(new Query(getTypeName(), ECQL.toFilter("EventCode = '051'")));
        // attribute query on a secondary index with a projection
        queries.add(new Query(getTypeName(), ECQL.toFilter("EventCode = '051' AND " + during),
          new String[]{ "GLOBALEVENTID", "dtg", "geom" }));
        
        this.queries = Collections.unmodifiableList(queries);
      } catch (CQLException e) {
        throw new RuntimeException("Error creating filter:", e);
      }
    }
    return queries;
  }
  
  @Override
  public Filter getSubsetFilter() {
    if (subsetFilter == null) {
      // Get a FilterFactory2 to build up our query
      FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
      
      // most of the data is from 2018-01-01
      ZonedDateTime dateTime = ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
      Date start = Date.from(dateTime.minusDays(1).toInstant());
      Date end = Date.from(dateTime.plusDays(1).toInstant());
      
      // note: BETWEEN is inclusive, while DURING is exclusive
      Filter dateFilter = ff.between(ff.property("dtg"), ff.literal(start), ff.literal(end));
      
      // bounding box over small portion of the eastern United States
      Filter spatialFilter = ff.bbox("geom",-83,33,-80,35,"EPSG:4326");
      
      // Now we can combine our filters using a boolean AND operator
      subsetFilter = ff.and(dateFilter, spatialFilter);
      
      // note the equivalent using ECQL would be:
      // ECQL.toFilter("bbox(geom,-83,33,-80,35) AND dtg between '2017-12-31T00:00:00.000Z' and '2018-01-02T00:00:00.000Z'");
    }
    return subsetFilter;
  }
}
