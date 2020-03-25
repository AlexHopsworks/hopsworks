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

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@XmlRootElement
public class ElasticFeaturestoreItemDTO {
  //base fields
  private String name;
  private String description;
  private Date created;
  private Integer version;
  //access fields
  private Integer parentProjectId;
  private String parentProjectName;
  private Map<Integer, String> accessProjects = new HashMap<>();
  
  public ElasticFeaturestoreItemDTO() {
  }
  
  public ElasticFeaturestoreItemDTO(String name, Integer version, Integer parentProjectId, String parentProjectName) {
    this.name = name;
    this.description = "";
    this.created = null;
    this.version = version;
    this.parentProjectId = parentProjectId;
    this.parentProjectName = parentProjectName;
  }
  
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
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
  
  public Integer getVersion() {
    return version;
  }
  
  public void setVersion(Integer version) {
    this.version = version;
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
