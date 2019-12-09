/*
 * This file is part of Hopsworks
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.common.provenance.ops.apiToElastic;

import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.core.apiToElastic.ProvParser;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import java.util.EnumSet;
import java.util.logging.Level;

public class ProvOParser {
  public enum BaseField implements ProvParser.ElasticField {
    INODE_OPERATION,
    TIMESTAMP,
    ARCHIVE_LOC;
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum AuxField implements ProvParser.ElasticField {
    LOGICAL_TIME,
    R_TIMESTAMP,
    INODE_PATH,
    XATTR;
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  public enum FileOps implements ProvParser.Field {
    PROJECT_I_ID(ProvParser.BaseField.PROJECT_I_ID, new ProvParser.LongValParser()),
    DATASET_I_ID(ProvParser.BaseField.DATASET_I_ID, new ProvParser.LongValParser()),
    FILE_I_ID(ProvParser.BaseField.INODE_ID, new ProvParser.LongValParser()),
    FILE_NAME(ProvParser.BaseField.INODE_NAME, new ProvParser.StringValParser()),
    USER_ID(ProvParser.BaseField.USER_ID, new ProvParser.IntValParser()),
    APP_ID(ProvParser.BaseField.APP_ID, new ProvParser.StringValParser()),
    ML_TYPE(ProvParser.BaseField.ML_TYPE, new ProvParser.MLTypeValParser()),
    ML_ID(ProvParser.BaseField.ML_ID, new ProvParser.StringValParser()),
    FILE_OPERATION(BaseField.INODE_OPERATION, new FileOpValParser()),
    TIMESTAMP(BaseField.TIMESTAMP, new ProvParser.LongValParser());
  
    ProvParser.ElasticField elasticField;
    ProvParser.ValParser valParser;
    
    FileOps(ProvParser.ElasticField elasticField, ProvParser.ValParser valParser) {
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
  
  public enum FileOpsAux implements ProvParser.Field {
    FILE_NAME_LIKE(FileOps.FILE_NAME, ProvParser.FilterType.LIKE),
    TIMESTAMP_LT(FileOps.TIMESTAMP, ProvParser.FilterType.RANGE_LT),
    TIMESTAMP_LTE(FileOps.TIMESTAMP, ProvParser.FilterType.RANGE_LTE),
    TIMESTAMP_GT(FileOps.TIMESTAMP, ProvParser.FilterType.RANGE_GT),
    TIMESTAMP_GTE(FileOps.TIMESTAMP, ProvParser.FilterType.RANGE_GTE);
    
    FileOps base;
    ProvParser.FilterType filterType;
    
    FileOpsAux(FileOps base, ProvParser.FilterType filterType) {
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
  
  public static class FileOpValParser implements ProvParser.ValParser {
    
    @Override
    public String parse(Object o) throws IllegalArgumentException {
      try {
        if(o instanceof String) {
          return Provenance.FileOps.valueOf((String)o).name();
        } else {
          throw new IllegalArgumentException("expected string-ified version of FileOp");
        }
      } catch (NullPointerException | IllegalArgumentException e) {
        throw new IllegalArgumentException("expected:" + EnumSet.allOf(Provenance.FileOps.class) + "found " + o, e);
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
  
  public static ProvParser.Field extractField(String val)
    throws ProvenanceException {
    try {
      return FileOps.valueOf(val.toUpperCase());
    } catch (NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileOpsAux.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException ee) {
      //try next
    }
    StringBuilder supported = new StringBuilder();
    supported.append(EnumSet.allOf(FileOps.class));
    supported.append(EnumSet.allOf(FileOpsAux.class));
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
