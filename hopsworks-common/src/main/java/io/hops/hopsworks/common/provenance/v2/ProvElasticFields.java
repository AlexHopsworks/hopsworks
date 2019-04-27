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
package io.hops.hopsworks.common.provenance.v2;

import io.hops.hopsworks.common.provenance.util.CheckedFunction;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;

import java.util.EnumSet;
import java.util.Map;
import java.util.logging.Level;

public class ProvElasticFields {
  public static final String XATTR = "xattr_prov";
  public interface Field {
  }
  
  public enum FileBase implements Field {
    PROJECT_I_ID,
    DATASET_I_ID,
    PARENT_I_ID,
    INODE_ID,
    INODE_NAME,
    USER_ID,
    APP_ID,
    ENTRY_TYPE;
  
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum FileAux implements Field {
    PARTITION_ID,
    PROJECT_NAME;
  
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum FileStateBase implements Field {
    CREATE_TIMESTAMP,
    ML_TYPE,
    ML_ID;
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum FileStateAux implements Field {
    R_CREATE_TIMESTAMP;
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum FileOpsBase implements Field {
    INODE_OPERATION,
    TIMESTAMP,
    ARCHIVE_LOC;
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum FileOpsAux implements Field {
    ML_TYPE,
    ML_ID,
    LOGICAL_TIME,
    R_TIMESTAMP,
    INODE_PATH,
    XATTR;
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum AppState implements Field {
    APP_STATE,
    APP_ID;
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum XAttr implements Field {
    XATTR_PROV;
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum EntryType {
    STATE,
    OPERATION,
    ARCHIVE;
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public enum MLType {
    NONE,
    FEATURE,
    TRAINING_DATASET,
    EXPERIMENT,
    MODEL,
    FEATURE_PART,
    TRAINING_PART,
    EXPERIMENT_PART,
    MODEL_PART;
  
    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }
  
  public static MLType parseMLType(String val) throws GenericException {
    try {
      return MLType.valueOf(val.toUpperCase());
    } catch (NullPointerException | IllegalArgumentException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "ML_TYPE" + val + " not supported - supported:" + EnumSet.allOf(MLType.class),
        "exception extracting ML_TYPE");
    }
  }
  
  public static Field extractFileStateQueryResultFields(String val) throws GenericException {
    try {
      return FileBase.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileAux.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileStateBase.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileStateAux.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    
    StringBuilder supported = new StringBuilder();
    supported.append(EnumSet.allOf(FileBase.class));
    supported.append(EnumSet.allOf(FileAux.class));
    supported.append(EnumSet.allOf(FileStateBase.class));
    supported.append(EnumSet.allOf(FileStateAux.class));
    throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
      "file state items - param" + val + " not supported - supported:" + supported,
      "exception extracting SortBy param");
  }
  
  public static Field extractFileOpsQueryResultFields(String val) throws GenericException {
    try {
      return FileBase.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileAux.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileOpsBase.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileOpsAux.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    
    StringBuilder supported = new StringBuilder();
    supported.append(EnumSet.allOf(FileBase.class));
    supported.append(EnumSet.allOf(FileAux.class));
    supported.append(EnumSet.allOf(FileOpsBase.class));
    supported.append(EnumSet.allOf(FileOpsAux.class));
    throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
      "file ops items - param" + val + " not supported - supported:" + supported,
      "exception extracting SortBy param");
  }
  
  public static <C> C extractField(Map<String, Object> fields, Field field,
    CheckedFunction<Object, C, GenericException> parser) throws GenericException {
    Object val = fields.get(field.toString());
    try {
      return parser.apply(fields.remove(field.toString()));
    } catch (GenericException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "problem parsing field:" + field, "problem parsing field:" + field, e);
    }
  }
}
