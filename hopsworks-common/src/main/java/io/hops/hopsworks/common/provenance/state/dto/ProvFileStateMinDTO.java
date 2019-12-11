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
package io.hops.hopsworks.common.provenance.state.dto;

import io.hops.hopsworks.common.provenance.core.ProvExecution;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpElastic;
import io.hops.hopsworks.common.provenance.state.ProvTree;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ProvFileStateMinDTO
  implements ProvTree.State, ProvExecution.FootprintItem<Long> {
  private Long inodeId;
  private String inodeName;
  private Long projectInodeId;
  private Long datasetInodeId;
  private Provenance.MLType mlType;
  private String mlId;
  private Long parentInodeId;
  private Provenance.FootprintType footprintType;
  
  public ProvFileStateMinDTO() {}
  
  public ProvFileStateMinDTO(Long inodeId, String inodeName, Long projectInodeId, Long datasetInodeId,
    Provenance.MLType mlType, String mlId, Long parentInodeId) {
    this.inodeId = inodeId;
    this.inodeName = inodeName;
    this.projectInodeId = projectInodeId;
    this.datasetInodeId = datasetInodeId;
    this.mlType = mlType;
    this.mlId = mlId;
    this.parentInodeId = parentInodeId;
  }
  
  public static ProvFileStateMinDTO fromFileOp(ProvFileOpElastic op, Provenance.MLType mlType) {
    return new ProvFileStateMinDTO(op.getInodeId(), op.getInodeName(), op.getProjectInodeId(), op.getDatasetInodeId(),
      mlType, op.getMlId(), op.getParentInodeId());
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
  
  public Long getDatasetInodeId() {
    return datasetInodeId;
  }
  
  public void setDatasetInodeId(Long datasetInodeId) {
    this.datasetInodeId = datasetInodeId;
  }
  
  public Provenance.MLType getMlType() {
    return mlType;
  }
  
  public void setMlType(Provenance.MLType mlType) {
    this.mlType = mlType;
  }
  
  public String getMlId() {
    return mlId;
  }
  
  public void setMlId(String mlId) {
    this.mlId = mlId;
  }
  
  public Provenance.FootprintType getFootprintType() {
    return footprintType;
  }
  
  @Override
  public void setFootprintType(Provenance.FootprintType footprintType) {
    this.footprintType = footprintType;
  }
  
  @Override
  public Long getKey() {
    return inodeId;
  }
}
