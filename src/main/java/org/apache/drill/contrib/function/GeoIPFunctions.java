package org.apache.drill.contrib.function;

/*
 * Copyright 2001-2004 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;

import javax.inject.Inject;


public class GeoIPFunctions {

  @FunctionTemplate(
          name = "getCountryName",
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )

  public static class getCountryNameFunction implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder inputTextA;

    @Output
    NullableVarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      java.io.InputStream db = getClass().getClassLoader().getResourceAsStream("GeoLite2-Country.mmdb");
      try {
        reader = new com.maxmind.geoip2.DatabaseReader.Builder(db).withCache(new com.maxmind.db.CHMCache()).build();
      } catch (java.io.IOException e) {
        System.out.print("IOException encountered:  Could not read MaxMind DB");
      }
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String countryName = "Unknown";

      try {
        java.net.InetAddress addr = java.net.InetAddress.getByName(ip);
        com.maxmind.geoip2.model.CountryResponse country = reader.country(addr);
        com.maxmind.geoip2.record.Country c = country.getCountry();
        countryName = c.getName();
        if (countryName == null) {
          countryName = "Unknown";
        }

      } catch (Exception e) {
        countryName = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = countryName.getBytes().length;
      buffer.setBytes(0, countryName.getBytes());
    }
  }

  @FunctionTemplate(
          name = "getCountryISOCode",
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class getCountryISOFunction implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {

      java.io.InputStream db = getClass().getClassLoader().getResourceAsStream("GeoLite2-Country.mmdb");
      try {
        reader = new com.maxmind.geoip2.DatabaseReader.Builder(db).withCache(new com.maxmind.db.CHMCache()).build();
      } catch (java.io.IOException e) {
        System.out.print("IOException encountered:  Could not read MaxMind DB");
      }
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String countryName = "UNK";

      try {
        com.maxmind.geoip2.model.CountryResponse country = reader.country(java.net.InetAddress.getByName(ip));
        countryName = country.getCountry().getIsoCode();
        if (countryName == null) {
          countryName = "UNK";
        }

      } catch (Exception e) {
        countryName = "UNK";
      }
      out.buffer = buffer;
      out.start = 0;
      out.end = countryName.getBytes().length;
      buffer.setBytes(0, countryName.getBytes());
    }
  }


  @FunctionTemplate(
          name = "getCountryConfidence",
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class getCountryConfidenceFunction implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {

      java.io.InputStream db = getClass().getClassLoader().getResourceAsStream("GeoLite2-Country.mmdb");
      try {
        reader = new com.maxmind.geoip2.DatabaseReader.Builder(db).withCache(new com.maxmind.db.CHMCache()).build();
      } catch (java.io.IOException e) {
        System.out.print("IOException encountered:  Could not read MaxMind DB");
      }
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int confidence = 0;

      try {
        com.maxmind.geoip2.model.CountryResponse country = reader.country(java.net.InetAddress.getByName(ip));
        confidence = country.getCountry().getConfidence();

      } catch (Exception e) {
        confidence = 0;
      }

      out.value = confidence;
    }
  }


  @FunctionTemplate(
          name = "getCityName",
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )

  public static class getCityNameFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {

      java.io.InputStream db = getClass().getClassLoader().getResourceAsStream("GeoLite2-City.mmdb");

      try {
        reader = new com.maxmind.geoip2.DatabaseReader.Builder(db).withCache(new com.maxmind.db.CHMCache()).build();
      } catch (java.io.IOException e) {
        System.out.print("IOException encountered:  Could not read MaxMind DB");
      }
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String cityName = "";

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        cityName = city.getCity().getName();
        if (cityName == null) {
          cityName = "Unknown";
        }
      } catch (Exception e) {
        cityName = "Unknown";
      }
      out.buffer = buffer;
      out.start = 0;
      out.end = cityName.getBytes().length;
      buffer.setBytes(0, cityName.getBytes());
    }
  }

  @FunctionTemplate(
          name = "getCityConfidence",
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )

  public static class getCityConfidenceFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {

      java.io.InputStream db = getClass().getClassLoader().getResourceAsStream("GeoLite2-City.mmdb");

      try {
        reader = new com.maxmind.geoip2.DatabaseReader.Builder(db).withCache(new com.maxmind.db.CHMCache()).build();
      } catch (java.io.IOException e) {
        System.out.print("IOException encountered:  Could not read MaxMind DB");
      }
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int cityConfidence = 0;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        cityConfidence = city.getCity().getConfidence();
      } catch (Exception e) {
        cityConfidence = 0;
      }
      out.value = cityConfidence;
    }
  }

  @FunctionTemplate(
          name = "getLatitude",
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class getLatitudeFunction implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder inputTextA;

    @Output
    Float8Holder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      java.io.InputStream db = getClass().getClassLoader().getResourceAsStream("GeoLite2-City.mmdb");
      try {
        reader = new com.maxmind.geoip2.DatabaseReader.Builder(db).withCache(new com.maxmind.db.CHMCache()).build();
      } catch (java.io.IOException e) {
        System.out.print("IOException encountered:  Could not read MaxMind DB");
      }
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      double latitude = 0.0;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        latitude = location.getLatitude();

      } catch (Exception e) {
        latitude = 0.0;
      }

      out.value = latitude;

    }
  }

  @FunctionTemplate(
          name = "getLongitude",
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class getLongitudeFunction implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder inputTextA;

    @Output
    Float8Holder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    @Workspace
    java.io.File database;

    public void setup() {

      database = new java.io.File("/Users/cgivre/OneDrive/github/drillworkshop/GeoLite2/GeoLite2-City.mmdb");
      try {
        reader = new com.maxmind.geoip2.DatabaseReader.Builder(database).withCache(new com.maxmind.db.CHMCache()).build();
      } catch (java.io.IOException e) {
        System.out.print("IOException encountered:  Could not read MaxMind DB");
      }
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      double longitude = 0.0;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        longitude = location.getLongitude();

      } catch (Exception e) {
        longitude = 0.0;
      }

      out.value = longitude;

    }
  }


  @FunctionTemplate(
          name = "getPostalCode",
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class getPostalCodeFunction implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder inputTextA;

    @Output
    NullableVarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;


    public void setup() {

      java.io.InputStream db = getClass().getClassLoader().getResourceAsStream("GeoLite2-City.mmdb");
      try {
        reader = new com.maxmind.geoip2.DatabaseReader.Builder(db).withCache(new com.maxmind.db.CHMCache()).build();
      } catch (java.io.IOException e) {
        System.out.print("IOException encountered:  Could not read MaxMind DB");
      }
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String postalCode = "";

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Postal postal = city.getPostal();
        postalCode = postal.getCode();
        if (postalCode == null) {
          postalCode = "Unknown";
        }
      } catch (Exception e) {
        postalCode = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = postalCode.getBytes().length;
      buffer.setBytes(0, postalCode.getBytes());

    }
  }

  @FunctionTemplate(
          name = "getCoordPoint",
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class getCoordPointFunction implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder inputTextA;

    @Output
    VarBinaryHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;


    public void setup() {

      java.io.InputStream db = getClass().getClassLoader().getResourceAsStream("GeoLite2-City.mmdb");

      try {
        reader = new com.maxmind.geoip2.DatabaseReader.Builder(db).withCache(new com.maxmind.db.CHMCache()).build();
      } catch (java.io.IOException e) {
        System.out.print("IOException encountered:  Could not read MaxMind DB");
      }
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      double longitude = 0.0;
      double latitude = 0.0;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        longitude = location.getLongitude();
        latitude = location.getLatitude();

      } catch (Exception e) {
        latitude = 0.0;
        longitude = 0.0;
      }
      com.esri.core.geometry.ogc.OGCPoint point = new com.esri.core.geometry.ogc.OGCPoint(
              new com.esri.core.geometry.Point(longitude, latitude), com.esri.core.geometry.SpatialReference.create(4326));

      java.nio.ByteBuffer pointBytes = point.asBinary();
      out.buffer = buffer;
      out.start = 0;
      out.end = pointBytes.remaining();
      buffer.setBytes(0, pointBytes);

    }
  }

}

