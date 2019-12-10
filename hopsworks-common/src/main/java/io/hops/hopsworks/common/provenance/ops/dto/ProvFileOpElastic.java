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
package io.hops.hopsworks.common.provenance.ops.dto;

import io.hops.hopsworks.common.provenance.core.apiToElastic.ProvParser;
import io.hops.hopsworks.common.provenance.core.elastic.BasicElasticHit;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.elastic.prov.ProvHelper;
import io.hops.hopsworks.common.provenance.ops.apiToElastic.ProvOParser;
import io.hops.hopsworks.common.provenance.state.dto.ProvMLAssetAppStateDTO;
import io.hops.hopsworks.exceptions.ProvenanceException;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

@XmlRootElement
public class ProvFileOpElastic implements Comparator<ProvFileOpElastic>  {
  private String id;
  private float score;
  private Map<String, Object> map;
  
  private Long inodeId;
  private Provenance.FileOps inodeOperation;
  private String appId;
  private Integer userId;
  private Long parentInodeId;
  private Long projectInodeId;
  private Long datasetInodeId;
  private Integer logicalTime;
  private Long timestamp;
  private String readableTimestamp;
  private String inodeName;
  private String xattrName;
  private String xattrVal;
  private String inodePath;
  private Long partitionId;
  private String projectName;
  private ProvParser.DocSubType docSubType;
  private String mlId;
  private ProvMLAssetAppStateDTO appState;
  
  public ProvFileOpElastic() {}
  
  public static ProvFileOpElastic instance(BasicElasticHit hit, boolean soft) throws ProvenanceException {
    ProvFileOpElastic result = new ProvFileOpElastic();
    result.id = hit.getId();
    result.score = Float.isNaN(hit.getScore()) ? 0 : hit.getScore();
    result.map = hit.getSource();
    return instance(result, soft);
  }
  
  private static ProvFileOpElastic instance(ProvFileOpElastic result, boolean soft)
    throws ProvenanceException {
    Map<String, Object> auxMap = new HashMap<>(result.map);
    result.projectInodeId = ProvParser.extractElasticField(auxMap,
      ProvParser.BaseField.PROJECT_I_ID, ProvHelper.asLong(soft));
    result.datasetInodeId = ProvParser.extractElasticField(auxMap,
      ProvParser.BaseField.DATASET_I_ID, ProvHelper.asLong(soft));
    result.inodeId = ProvParser.extractElasticField(auxMap,
      ProvParser.BaseField.INODE_ID, ProvHelper.asLong(soft));
    result.appId = ProvParser.extractElasticField(auxMap,
      ProvParser.BaseField.APP_ID, ProvHelper.asString(soft));
    result.userId = ProvParser.extractElasticField(auxMap,
      ProvParser.BaseField.USER_ID, ProvHelper.asInt(soft));
    result.inodeName = ProvParser.extractElasticField(auxMap,
      ProvParser.BaseField.INODE_NAME, ProvHelper.asString(soft));
    result.inodeOperation = ProvParser.extractElasticField(auxMap,
      ProvOParser.BaseField.INODE_OPERATION, ProvHelper.asFileOp(soft));
    result.timestamp = ProvParser.extractElasticField(auxMap,
      ProvOParser.BaseField.TIMESTAMP, ProvHelper.asLong(soft));
    result.parentInodeId = ProvParser.extractElasticField(auxMap,
      ProvParser.BaseField.PARENT_I_ID, ProvHelper.asLong(soft));
    result.partitionId = ProvParser.extractElasticField(auxMap,
      ProvParser.AuxField.PARTITION_ID, ProvHelper.asLong(soft));
    result.projectName = ProvParser.extractElasticField(auxMap,
      ProvParser.AuxField.PROJECT_NAME, ProvHelper.asString(soft));
    result.mlId = ProvParser.extractElasticField(auxMap,
      ProvParser.BaseField.ML_ID, ProvHelper.asString(soft));
    result.docSubType = ProvParser.extractElasticField(auxMap,
      ProvParser.BaseField.ML_TYPE, ProvHelper.asDocSubType(soft));
    result.logicalTime = ProvParser.extractElasticField(auxMap,
      ProvOParser.AuxField.LOGICAL_TIME, ProvHelper.asInt(soft));
    result.readableTimestamp = ProvParser.extractElasticField(auxMap,
      ProvOParser.AuxField.R_TIMESTAMP, ProvHelper.asString(soft));
    ProvParser.extractElasticField(auxMap,
      ProvParser.BaseField.ENTRY_TYPE, ProvHelper.asString(soft));
    Map<String, String> xattrs = ProvParser.extractElasticField(auxMap,
      ProvParser.XAttrField.XATTR_PROV, ProvHelper.asXAttrMap(true));
    if(xattrs != null && xattrs.size() == 1) {
      Map.Entry<String, String> e = xattrs.entrySet().iterator().next();
      result.xattrName = e.getKey();
      result.xattrVal = e.getValue();
    }
    return result;
  }
  
  @Override
  public int compare(ProvFileOpElastic o1, ProvFileOpElastic o2) {
    return Float.compare(o2.getScore(), o1.getScore());
  }
  
  
  public String getId() {
    return id;
  }
  
  public void setId(String id) {
    this.id = id;
  }
  
  public float getScore() {
    return score;
  }
  
  public void setScore(float score) {
    this.score = score;
  }
  
  public Map<String, String> getMap() {
    //flatten hits (remove nested json objects) to make it more readable
    Map<String, String> refined = new HashMap<>();
    
    if (this.map != null) {
      for (Map.Entry<String, Object> entry : this.map.entrySet()) {
        //convert value to string
        String value = (entry.getValue() == null) ? "null" : entry.getValue().toString();
        refined.put(entry.getKey(), value);
      }
    }
    
    return refined;
  }
  
  public void setMap(Map<String, Object> map) {
    this.map = map;
  }
  
  
  public Long getInodeId() {
    return inodeId;
  }
  
  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }
  
  public Provenance.FileOps getInodeOperation() {
    return inodeOperation;
  }
  
  public void setInodeOperation(Provenance.FileOps inodeOperation) {
    this.inodeOperation = inodeOperation;
  }
  
  public String getAppId() {
    return appId;
  }
  
  public void setAppId(String appId) {
    this.appId = appId;
  }
  
  public Integer getLogicalTime() {
    return logicalTime;
  }
  
  public void setLogicalTime(Integer logicalTime) {
    this.logicalTime = logicalTime;
  }
  
  public Long getTimestamp() {
    return timestamp;
  }
  
  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }
  
  public String getReadableTimestamp() {
    return readableTimestamp;
  }
  
  public void setReadableTimestamp(String readableTimestamp) {
    this.readableTimestamp = readableTimestamp;
  }
  
  public String getInodeName() {
    return inodeName;
  }
  
  public void setInodeName(String inodeName) {
    this.inodeName = inodeName;
  }
  
  public String getXattrName() {
    return xattrName;
  }
  
  public void setXattrName(String xattrName) {
    this.xattrName = xattrName;
  }
  
  public String getXattrVal() {
    return xattrVal;
  }
  
  public void setXattrVal(String xattrVal) {
    this.xattrVal = xattrVal;
  }
  
  public String getInodePath() {
    return inodePath;
  }
  
  public void setInodePath(String inodePath) {
    this.inodePath = inodePath;
  }
  
  public Long getParentInodeId() {
    return parentInodeId;
  }
  
  public void setParentInodeId(Long parentInodeId) {
    this.parentInodeId = parentInodeId;
  }
  
  public Integer getUserId() {
    return userId;
  }
  
  public void setUserId(Integer userId) {
    this.userId = userId;
  }
  
  public Long getPartitionId() {
    return partitionId;
  }
  
  public void setPartitionId(Long partitionId) {
    this.partitionId = partitionId;
  }
  
  public Long getProjectInodeId() {
    return projectInodeId;
  }
  
  public void setProjectInodeId(Long projectInodeId) {
    this.projectInodeId = projectInodeId;
  }
  
  public ProvMLAssetAppStateDTO getAppState() {
    return appState;
  }
  
  public void setAppState(ProvMLAssetAppStateDTO appState) {
    this.appState = appState;
  }
  
  public ProvParser.DocSubType getDocSubType() {
    return docSubType;
  }
  
  public void setDocSubType(ProvParser.DocSubType docSubType) {
    this.docSubType = docSubType;
  }
  
  public String getMlId() {
    return mlId;
  }
  
  public void setMlId(String mlId) {
    this.mlId = mlId;
  }
  
  public String getProjectName() {
    return projectName;
  }
  
  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }
  
  public Long getDatasetInodeId() {
    return datasetInodeId;
  }
  
  public void setDatasetInodeId(Long datasetInodeId) {
    this.datasetInodeId = datasetInodeId;
  }
}
