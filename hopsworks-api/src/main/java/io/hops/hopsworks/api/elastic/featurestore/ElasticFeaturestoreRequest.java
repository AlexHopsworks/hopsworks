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
package io.hops.hopsworks.api.elastic.featurestore;

import io.hops.hopsworks.common.elastic.FeaturestoreDocType;

public class ElasticFeaturestoreRequest {
  private String term;
  private FeaturestoreDocType docType;
  private int from;
  private int size;
  
  public ElasticFeaturestoreRequest() {
  }
  
  public ElasticFeaturestoreRequest(String term, FeaturestoreDocType docType, int from, int size) {
    this.term = term;
    this.docType = docType;
    this.from = from;
    this.size = size;
  }
  
  public String getTerm() {
    return term;
  }
  
  public void setTerm(String term) {
    this.term = term;
  }
  
  public FeaturestoreDocType getDocType() {
    return docType;
  }
  
  public void setDocType(FeaturestoreDocType docType) {
    this.docType = docType;
  }
  
  public int getFrom() {
    return from;
  }
  
  public void setFrom(int from) {
    this.from = from;
  }
  
  public int getSize() {
    return size;
  }
  
  public void setSize(int size) {
    this.size = size;
  }
}
