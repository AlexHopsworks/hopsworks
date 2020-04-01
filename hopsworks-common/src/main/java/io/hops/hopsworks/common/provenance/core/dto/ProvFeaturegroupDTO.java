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
package io.hops.hopsworks.common.provenance.core.dto;

import io.hops.hopsworks.common.provenance.core.ProvXAttrs;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class ProvFeaturegroupDTO {
  @XmlRootElement
  public static class Base {
    @XmlElement(nillable = false, name = ProvXAttrs.Featurestore.FEATURESTORE_ID)
    private Integer featurestoreId;
    
    @XmlElement(nillable = false, name = ProvXAttrs.Featurestore.FG_FEATURES)
    private List<String> features = new LinkedList<>();
  
    public Base() {}
  
    public Base(Integer featurestoreId) {
      this(featurestoreId, new LinkedList<>());
    }
  
    public Base(Integer featurestoreId, List<String> features) {
      this.featurestoreId = featurestoreId;
      this.features = features;
    }
  
    public Integer getFeaturestoreId() {
      return featurestoreId;
    }
  
    public void setFeaturestoreId(Integer featurestoreId) {
      this.featurestoreId = featurestoreId;
    }
  
    public List<String> getFeatures() {
      return features;
    }
  
    public void setFeatures(List<String> features) {
      this.features = features;
    }
  
    public void addFeature(String feature) {
      features.add(feature);
    }
  
    @Override
    public String toString() {
      return "Base{" +
        "featurestoreId=" + featurestoreId +
        ", features=" + features +
        '}';
    }
  }
  
  @XmlRootElement
  public static class Featuregroup extends Base {
    @XmlElement(nillable = true, name = ProvXAttrs.Featurestore.DESCRIPTION)
    private String description;
    @XmlElement(nillable = true, name = ProvXAttrs.Featurestore.CREATE_DATE)
    private Long createDate;
    @XmlElement(nillable = true, name = ProvXAttrs.Featurestore.CREATOR)
    private String creator;
  
    public Featuregroup() {
      super();
    }
  
    public Featuregroup(Integer featurestoreId, String description, Date createDate, String creator) {
      this(featurestoreId, description, createDate, creator, new LinkedList<>());
    }
  
    public Featuregroup(Integer featurestoreId, String description,
      Date createDate, String creator, List<String> features) {
      super(featurestoreId, features);
      this.description = description;
      this.createDate = createDate.getTime();
      this.creator = creator;
    }
  
    public String getDescription() {
      return description;
    }
  
    public void setDescription(String description) {
      this.description = description;
    }
  
    public Long getCreateDate() {
      return createDate;
    }
  
    public void setCreateDate(Long createDate) {
      this.createDate = createDate;
    }
  
    public String getCreator() {
      return creator;
    }
  
    public void setCreator(String creator) {
      this.creator = creator;
    }
  
    @Override
    public String toString() {
      return super.toString() + "Extended{" +
        "description='" + description + '\'' +
        ", createDate=" + createDate +
        ", creator='" + creator + '\'' +
        '}';
    }
  }
  
  @XmlRootElement
  public static class TrainingDataset extends Base {
    @XmlElement(nillable = false, name = ProvXAttrs.Featurestore.NAME)
    private String name;
    @XmlElement(nillable = false, name = ProvXAttrs.Featurestore.VERSION)
    private Integer version;
  
    public TrainingDataset() {
      super();
    }
    
    public TrainingDataset(Integer featurestoreId, String name, Integer version) {
      super(featurestoreId);
      this.name = name;
      this.version = version;
    }
    public String getName() {
      return name;
    }
  
    public void setName(String name) {
      this.name = name;
    }
  
    public Integer getVersion() {
      return version;
    }
  
    public void setVersion(Integer version) {
      this.version = version;
    }
  }
}
