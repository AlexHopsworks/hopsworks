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
package io.hops.hopsworks.common.provenance.core;

public class ProvXAttrs {
  public static final String PROV_XATTR = "provenance";
  public static final String PROV_XATTR_CORE = PROV_XATTR + ".core";
  public static final String PROV_CORE_TYPE_KEY = "prov_type";
  public static final String PROV_CORE_PROJECT_IID_KEY = "project_iid";
  public static final String PROV_CORE_META_STATUS_KEY = "meta_status";
  public static final String PROV_CORE_STATUS_KEY = "prov_status";
  
  public static final String PROV_XATTR_EXPERIMENT_SUMMARY = PROV_XATTR + ".experiment_summary";
  public static final String PROV_EXP_EXECUTABLE = "executable";
  public static final String PROV_EXP_ENVIRONMENT = "environment";
  public static final String PROV_XATTR_MODEL = PROV_XATTR + ".model";
  
  public static final String ELASTIC_XATTR = "xattr";
  
  public static class Featurestore {
   
    public static final String FEATURESTORE = "featurestore";
    public static final String FEATURESTORE_ID = "featurestore_id";
    public static final String NAME = "name";
    public static final String VERSION = "version";
    public static final String FG_FEATURES = "fg_features";
    public static final String TD_FEATURES = "td_features";
    public static final String DESCRIPTION = "description";
    public static final String CREATE_DATE = "create_date";
    public static final String CREATOR = "creator";
  
    public static final String TAGS = "tags";
  
    public static String getFeaturestoreXAttrKey() {
      return PROV_XATTR
        + "." + Featurestore.FEATURESTORE;
    }
    public static String getFeaturestoreElasticKey() {
      return ELASTIC_XATTR
        + "." + Featurestore.FEATURESTORE;
    }
    
    public static String getFeaturestoreElasticKey(String key) {
      return getFeaturestoreElasticKey() + "." + key;
    }
    
    public static String getTagsElasticKey() {
      return ProvXAttrs.ELASTIC_XATTR
        + "." + Featurestore.TAGS
        + ".key";
    }
  
    public static String getTagsElasticValue() {
      return ProvXAttrs.ELASTIC_XATTR
        + "." + Featurestore.TAGS
        + ".value";
    }
  }
}
