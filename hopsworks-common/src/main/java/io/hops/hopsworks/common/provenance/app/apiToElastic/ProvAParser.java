/*
 * This file is part of Hopsworks
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
 *
 * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */
package io.hops.hopsworks.common.provenance.app.apiToElastic;

import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.core.apiToElastic.ProvParser;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.javatuples.Pair;

import java.util.EnumSet;
import java.util.logging.Level;

public class ProvAParser {
  public enum BaseField implements ProvParser.ElasticField {
    APP_STATE,
    APP_ID;
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum ExpansionApp implements ProvParser.Field {
    APP_STATE(BaseField.APP_STATE, new AppStateValParser()),
    APP_ID(BaseField.APP_ID, new ProvParser.StringValParser());
    
    public final BaseField elasticField;
    public final ProvParser.ValParser valParser;
    
    ExpansionApp(BaseField elasticField, ProvParser.ValParser valParser) {
      this.elasticField = elasticField;
      this.valParser = valParser;
    }
    
    @Override
    public String elasticFieldName() {
      return elasticField.toString().toLowerCase();
    }
    
    @Override
    public String queryFieldName() {
      return name().toLowerCase();
    }
    
    @Override
    public ProvParser.FilterType filterType() {
      return ProvParser.FilterType.EXACT;
    }
    
    @Override
    public ProvParser.ValParser filterValParser() {
      return valParser;
    }
  }
  
  public static class AppStateValParser implements ProvParser.ValParser {
    @Override
    public String parse(Object o) throws IllegalArgumentException {
      try {
        if(o instanceof String) {
          return Provenance.AppState.valueOf((String)o).name();
        } else {
          throw new IllegalArgumentException("expected string-ified version of AppState");
        }
      } catch (NullPointerException | IllegalArgumentException e) {
        throw new IllegalArgumentException("expected:" + EnumSet.allOf(Provenance.AppState.class) + "found " + o, e);
      }
    }
  }
  
  public static Pair<ProvParser.Field, Object> extractFilter(String param) throws ProvenanceException {
    String rawFilter;
    String rawVal;
    if (param.contains(":")) {
      int aux = param.indexOf(':');
      rawFilter = param.substring(0, aux);
      rawVal = param.substring(aux+1);
    } else {
      rawFilter = param;
      rawVal = "true";
    }
    ProvParser.Field field = extractField(rawFilter);
    Object val = field.filterValParser().parse(rawVal);
    return Pair.with(field, val);
  }
  
  public static ProvParser.Field extractField(String val) throws ProvenanceException {
    try {
      return ExpansionApp.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      StringBuilder supported = new StringBuilder();
      supported.append(EnumSet.allOf(ExpansionApp.class));
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.BAD_REQUEST, Level.INFO,
        "sort param" + val + " not supported - supported:" + supported,
        "exception extracting SortBy param", e);
    }
  }
}
