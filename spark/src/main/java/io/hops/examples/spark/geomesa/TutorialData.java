/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package io.hops.examples.spark.geomesa;

import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.util.List;

public interface TutorialData {
  
  String getTypeName();
  SimpleFeatureType getSimpleFeatureType();
  List<SimpleFeature> getTestData();
  List<Query> getTestQueries();
  Filter getSubsetFilter();
  
  
  /**
   * Creates a geotools filter based on a bounding box and date range
   *
   * @param geomField geometry attribute name
   * @param x0 bounding box min x value
   * @param y0 bounding box min y value
   * @param x1 bounding box max x value
   * @param y1 bounding box max y value
   * @param dateField date attribute name
   * @param t0 minimum time, exclusive, in the format "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
   * @param t1 maximum time, exclusive, in the format "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
   * @param attributesQuery any additional query string, or null
   * @return filter object
   * @throws CQLException if invalid CQL
   */
  static Filter createFilter(String geomField, double x0, double y0, double x1, double y1,
    String dateField, String t0, String t1,
    String attributesQuery) throws CQLException {
    
    // there are many different geometric predicates that might be used;
    // here, we just use a bounding-box (BBOX) predicate as an example.
    // this is useful for a rectangular query area
    String cqlGeometry = "BBOX(" + geomField + ", " + x0 + ", " + y0 + ", " + x1 + ", " + y1 + ")";
    
    // there are also quite a few temporal predicates; here, we use a
    // "DURING" predicate, because we have a fixed range of times that
    // we want to query
    String cqlDates = "(" + dateField + " DURING " + t0 + "/" + t1 + ")";
    
    // there are quite a few predicates that can operate on other attribute
    // types; the GeoTools Filter constant "INCLUDE" is a default that means
    // to accept everything
    String cqlAttributes = attributesQuery == null ? "INCLUDE" : attributesQuery;
    
    String cql = cqlGeometry + " AND " + cqlDates  + " AND " + cqlAttributes;
    
    // we use geotools ECQL class to parse a CQL string into a Filter object
    return ECQL.toFilter(cql);
  }
}
