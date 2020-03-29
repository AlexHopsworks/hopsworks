/*
 * This file is part of Hopsworks
 * Copyright (C) 2020, Logical Clocks AB. All rights reserved
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

import io.hops.hopsworks.common.elastic.FeaturestoreElasticHit;
import io.hops.hopsworks.common.provenance.core.ProvXAttrs;
import io.hops.hopsworks.common.provenance.core.dto.ProvFeaturegroupDTO;
import io.hops.hopsworks.common.provenance.core.dto.ProvTrainingDatasetDTO;
import io.hops.hopsworks.common.util.HopsworksJAXBContext;
import io.hops.hopsworks.exceptions.GenericException;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@XmlRootElement
public class ElasticFeaturestoreItemDTO {
  private String elasticId;
  //base fields
  private String name;
  private String description;
  private Date created;
  private String creator;
  private Integer featurestoreId;
  private Integer version;
  //access fields
  private Integer parentProjectId;
  private String parentProjectName;
  private Map<Integer, String> accessProjects = new HashMap<>();
  
  public ElasticFeaturestoreItemDTO() {
  }
  
  public static ElasticFeaturestoreItemDTO fromFeaturegroup(FeaturestoreElasticHit hit,
    HopsworksJAXBContext converter) throws GenericException {
    ElasticFeaturestoreItemDTO item = new ElasticFeaturestoreItemDTO();
    item.elasticId = hit.getId();
    item.name = hit.getName();
    item.version = hit.getVersion();
    item.parentProjectId = hit.getProjectId();
    item.parentProjectName = hit.getProjectName();
    for(Map.Entry<String, Object> e : hit.getXattrs().entrySet()) {
      switch (e.getKey()) {
        case ProvXAttrs.Featurestore.FEATUREGROUP: {
          ProvFeaturegroupDTO.Extended fg
            = converter.unmarshal(e.getValue().toString(), ProvFeaturegroupDTO.Extended.class);
          item.featurestoreId = fg.getFeaturestoreId();
          item.description = fg.getDescription();
          item.created = new Date(fg.getCreateDate());
          item.creator = fg.getCreator();
        } break;
      }
    }
    return item;
  }
  
  public static ElasticFeaturestoreItemDTO fromTrainingDataset(FeaturestoreElasticHit hit,
    HopsworksJAXBContext converter) throws GenericException {
    ElasticFeaturestoreItemDTO item = new ElasticFeaturestoreItemDTO();
    item.elasticId = hit.getId();
    item.name = hit.getName();
    item.version = hit.getVersion();
    item.parentProjectId = hit.getProjectId();
    item.parentProjectName = hit.getProjectName();
    for(Map.Entry<String, Object> e : hit.getXattrs().entrySet()) {
      switch (e.getKey()) {
        case ProvXAttrs.Featurestore.TRAINING_DATASET: {
          ProvTrainingDatasetDTO td
            = converter.unmarshal(e.getValue().toString(), ProvTrainingDatasetDTO.class);
          item.featurestoreId = td.getFeaturestoreId();
          item.description = td.getDescription();
          item.created = new Date(td.getCreateDate());
          item.creator = td.getCreator();
        } break;
      }
    }
    return item;
  }
  
  public String getElasticId() {
    return elasticId;
  }
  
  public void setElasticId(String elasticId) {
    this.elasticId = elasticId;
  }
  
  public Integer getFeaturestoreId() {
    return featurestoreId;
  }
  
  public void setFeaturestoreId(Integer featurestoreId) {
    this.featurestoreId = featurestoreId;
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
  
  public String getDescription() {
    return description;
  }
  
  public void setDescription(String description) {
    this.description = description;
  }
  
  public Date getCreated() {
    return created;
  }
  
  public void setCreated(Date created) {
    this.created = created;
  }
  
  public String getCreator() {
    return creator;
  }
  
  public void setCreator(String creator) {
    this.creator = creator;
  }
  
  public Integer getParentProjectId() {
    return parentProjectId;
  }
  
  public void setParentProjectId(Integer parentProjectId) {
    this.parentProjectId = parentProjectId;
  }
  
  public String getParentProjectName() {
    return parentProjectName;
  }
  
  public void setParentProjectName(String parentProjectName) {
    this.parentProjectName = parentProjectName;
  }
  
  public Map<Integer, String> getAccessProjects() {
    return accessProjects;
  }
  
  public void setAccessProjects(Map<Integer, String> accessProjects) {
    this.accessProjects = accessProjects;
  }
  
  public void addAccessProject(Integer projectId, String projectName) {
    accessProjects.put(projectId, projectName);
  }
}
