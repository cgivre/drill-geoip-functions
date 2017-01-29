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

import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;

public class GeoIPFunctions {

    @FunctionTemplate(
        name = "getCountryName",
        scope = FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
    )

    public static class getCountryNameFunction implements DrillSimpleFunc {

        @Param
        VarCharHolder inputTextA;

        @Output
        VarCharHolder out;

        @Inject
        DrillBuf buffer;

        @Workspace
        com.maxmind.geoip2.DatabaseReader reader;
        java.io.File database;


        public void setup() {

            database = new File("/Users/cgivre/OneDrive/github/drillworkshop/GeoLite2/GeoIP2-City.mmdb");
            try {
                reader = new com.maxmind.geoip2.DatabaseReader.Builder(database).build();
            } catch (IOException e) {
                System.out.print("IOException encountered:  Could not read MaxMind DB");
            }
        }


        public void eval() {
            String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
            String countryName = "";

            try {
                CountryResponse country = reader.country(java.net.InetAddress.getByName(ip));
                countryName = country.getCountry().getName();
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
        java.io.File database;


        public void setup() {

            database = new File("/Users/cgivre/OneDrive/github/drillworkshop/GeoLite2/GeoIP2-City.mmdb");

            try {
                reader = new com.maxmind.geoip2.DatabaseReader.Builder(database).build();
            } catch (IOException e) {
                System.out.print("IOException encountered:  Could not read MaxMind DB");
            }
        }

        public void eval() {
            String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
            String cityName = "";

            try {
                CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
                cityName = city.getCity().getName();
            } catch (Exception e) {
                cityName = "Unknown";
            }
            out.buffer = buffer;
            out.start = 0;
            out.end = cityName.getBytes().length;
            buffer.setBytes(0, cityName.getBytes());
        }
    }

}

