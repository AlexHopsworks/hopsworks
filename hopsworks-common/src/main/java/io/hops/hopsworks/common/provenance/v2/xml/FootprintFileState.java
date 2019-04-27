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
package io.hops.hopsworks.common.provenance.v2.xml;

import io.hops.hopsworks.common.provenance.ProvenanceController;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class FootprintFileState implements ProvenanceController.BasicFileState {
  private Long inodeId;
  private String inodeName;
  private Long parentInodeId;
  private Long projectInodeId;
  
  public FootprintFileState() {}
  
  public FootprintFileState(Long inodeId, String inodeName, Long parentInodeId, Long projectInodeId) {
    this.inodeId = inodeId;
    this.inodeName = inodeName;
    this.parentInodeId = parentInodeId;
    this.projectInodeId = projectInodeId;
  }
  
  @Override
  public Long getInodeId() {
    return inodeId;
  }
  
  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }
  
  @Override
  public String getInodeName() {
    return inodeName;
  }
  
  public void setInodeName(String inodeName) {
    this.inodeName = inodeName;
  }
  
  @Override
  public Long getParentInodeId() {
    return parentInodeId;
  }
  
  public void setParentInodeId(Long parentInodeId) {
    this.parentInodeId = parentInodeId;
  }
  
  public Long getProjectInodeId() {
    return projectInodeId;
  }
  
  public void setProjectInodeId(Long projectInodeId) {
    this.projectInodeId = projectInodeId;
  }
  
  @Override
  public boolean isProject() {
    return projectInodeId == inodeId;
  }
}
