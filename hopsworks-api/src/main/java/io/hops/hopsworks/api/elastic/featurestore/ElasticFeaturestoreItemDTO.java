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

import org.javatuples.Pair;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

@XmlRootElement
public class ElasticFeaturestoreItemDTO {
  //base fields
  private String name;
  private String description;
  private Date created;
  private Integer version;
  //access fields
  private Pair<Integer, String> parentProject;
  private List<Pair<Integer, String>> accessProjects = new LinkedList<>();
  
  public ElasticFeaturestoreItemDTO() {
  }
  
  public ElasticFeaturestoreItemDTO(String name, Integer version, Integer parentProjectId, String parentProjectName) {
    this.name = name;
    this.description = "";
    this.created = null;
    this.version = version;
    this.parentProject = Pair.with(parentProjectId, parentProjectName);
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
  
  public Pair<Integer, String> getParentProject() {
    return parentProject;
  }
  
  public void setParentProject(Pair<Integer, String> parentProject) {
    this.parentProject = parentProject;
  }
  
  public List<Pair<Integer, String>> getAccessProjects() {
    return accessProjects;
  }
  
  public void setAccessProjects(List<Pair<Integer, String>> accessProjects) {
    this.accessProjects = accessProjects;
  }
  
  public void addAccessProject(Integer projectId, String projectName) {
    accessProjects.add(Pair.with(projectId, projectName));
  }
}
