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
package io.hops.hopsworks.common.provenance.xml;

import io.hops.hopsworks.common.provenance.core.ProvExecution;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.elastic.prov.ProvFileOpElastic;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ProvMLStateMinDTO implements ProvExecution.FootprintItem<String> {
  private Long projectInodeId;
  private Long datasetInodeId;
  private Provenance.MLType mlType;
  private String mlId;
  private String appId;
  private Provenance.FootprintType footprintType;
  
  public ProvMLStateMinDTO() {
  }
  
  public ProvMLStateMinDTO(Long projectInodeId, Long datasetInodeId, Provenance.MLType mlType, String mlId,
    String appId) {
    this.projectInodeId = projectInodeId;
    this.datasetInodeId = datasetInodeId;
    this.mlType = mlType;
    this.mlId = mlId;
    this.appId = appId;
  }
  
  public static ProvMLStateMinDTO fromFileOp(ProvFileOpElastic op, Provenance.MLType mlType) {
    return new ProvMLStateMinDTO(op.getProjectInodeId(), op.getDatasetInodeId(), mlType, op.getMlId(), op.getAppId());
  }
  
  public Long getProjectInodeId() {
    return projectInodeId;
  }
  
  public void setProjectInodeId(Long projectInodeId) {
    this.projectInodeId = projectInodeId;
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
  
  public String getAppId() {
    return appId;
  }
  
  public void setAppId(String appId) {
    this.appId = appId;
  }
  
  @Override
  public void setFootprintType(Provenance.FootprintType footprintType) {
    this.footprintType = footprintType;
  }
  
  //ExecutionFootprint.FootprintItem<String>
  @Override
  public String getKey() {
    return mlId;
  }
}
