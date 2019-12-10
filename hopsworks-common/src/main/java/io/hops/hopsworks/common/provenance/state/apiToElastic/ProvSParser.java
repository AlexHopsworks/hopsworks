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
package io.hops.hopsworks.common.provenance.state.apiToElastic;

import io.hops.hopsworks.common.provenance.core.apiToElastic.ProvParser;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import java.util.EnumSet;
import java.util.logging.Level;

public class ProvSParser {
  public enum BaseField implements ProvParser.ElasticField {
    CREATE_TIMESTAMP;
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum AuxField implements ProvParser.ElasticField {
    R_CREATE_TIMESTAMP;
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum FileState implements ProvParser.Field {
    PROJECT_I_ID(ProvParser.BaseField.PROJECT_I_ID, new ProvParser.LongValParser()),
    DATASET_I_ID(ProvParser.BaseField.DATASET_I_ID, new ProvParser.LongValParser()),
    FILE_I_ID(ProvParser.BaseField.INODE_ID, new ProvParser.LongValParser()),
    FILE_NAME(ProvParser.BaseField.INODE_NAME, new ProvParser.StringValParser()),
    USER_ID(ProvParser.BaseField.USER_ID, new ProvParser.IntValParser()),
    APP_ID(ProvParser.BaseField.APP_ID, new ProvParser.StringValParser()),
    ML_TYPE(ProvParser.BaseField.ML_TYPE, new ProvParser.MLTypeValParser()),
    ML_ID(ProvParser.BaseField.ML_ID, new ProvParser.StringValParser()),
    CREATE_TIMESTAMP(BaseField.CREATE_TIMESTAMP, new ProvParser.LongValParser());
  
    ProvParser.ElasticField elasticField;
    ProvParser.ValParser filterValParser;
    
    FileState(ProvParser.ElasticField elasticField, ProvParser.ValParser filterValParser) {
      this.elasticField = elasticField;
      this.filterValParser = filterValParser;
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
      return filterValParser;
    }
  }
  
  public enum FileStateAux implements ProvParser.Field {
    FILE_NAME_LIKE(FileState.FILE_NAME, ProvParser.FilterType.LIKE),
    CREATE_TIMESTAMP_LT(FileState.CREATE_TIMESTAMP, ProvParser.FilterType.RANGE_LT),
    CREATE_TIMESTAMP_LTE(FileState.CREATE_TIMESTAMP, ProvParser.FilterType.RANGE_LTE),
    CREATE_TIMESTAMP_GT(FileState.CREATE_TIMESTAMP, ProvParser.FilterType.RANGE_GT),
    CREATE_TIMESTAMP_GTE(FileState.CREATE_TIMESTAMP, ProvParser.FilterType.RANGE_GTE),
    CREATETIME(FileState.CREATE_TIMESTAMP, ProvParser.FilterType.EXACT);
    
    FileState base;
    ProvParser.FilterType filterType;
    
    FileStateAux(FileState base, ProvParser.FilterType filterType) {
      this.base = base;
      this.filterType = filterType;
    }
    
    @Override
    public String elasticFieldName() {
      return base.elasticFieldName();
    }
    
    @Override
    public String queryFieldName() {
      return base.elasticFieldName();
    }
    
    @Override
    public ProvParser.FilterType filterType() {
      return filterType;
    }
    
    @Override
    public ProvParser.ValParser filterValParser() {
      return base.filterValParser();
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
      return FileState.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
    }
    try {
      return FileStateAux.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
    }
    StringBuilder supported = new StringBuilder();
    supported.append(EnumSet.allOf(FileState.class));
    supported.append(EnumSet.allOf(FileStateAux.class));
    throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.BAD_REQUEST, Level.INFO,
      "filter param" + val + " not supported - supported:" + supported,
      "exception extracting SortBy param");
  }
  
  public static Pair<ProvParser.Field, SortOrder> extractSort(String param) throws ProvenanceException {
    String rawSortField;
    String rawSortOrder;
    if (param.contains(":")) {
      int aux = param.indexOf(':');
      rawSortField = param.substring(0, aux);
      rawSortOrder = param.substring(aux+1);
    } else {
      rawSortField = param;
      rawSortOrder = "ASC";
    }
    ProvParser.Field sortField = extractField(rawSortField);
    SortOrder sortOrder = ProvParser.extractSortOrder(rawSortOrder);
    return Pair.with(sortField, sortOrder);
  }
}
