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
package io.hops.hopsworks.common.provenance.elastic;

import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.xml.ProvMLAssetAppStateDTO;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvElasticFields;
import io.hops.hopsworks.exceptions.ProvenanceException;
import org.elasticsearch.search.SearchHit;

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
  private ProvElasticFields.DocSubType docSubType;
  private String mlId;
  private ProvMLAssetAppStateDTO appState;
  
  public ProvFileOpElastic() {}
  
  public static ProvFileOpElastic instance(SearchHit hit, boolean soft) throws ProvenanceException {
    ProvFileOpElastic result = new ProvFileOpElastic();
    result.id = hit.getId();
    result.score = Float.isNaN(hit.getScore()) ? 0 : hit.getScore();
    return instance(result, hit.getSourceAsMap(), soft);
  }
  
  public static ProvFileOpElastic instance(String id, Map<String, Object> sourceMap, boolean soft)
    throws ProvenanceException {
    ProvFileOpElastic result = new ProvFileOpElastic();
    result.id = id;
    result.score = 0;
    return instance(result, sourceMap, soft);
  }
  
  private static ProvFileOpElastic instance(ProvFileOpElastic result, Map<String, Object> sourceMap, boolean soft)
    throws ProvenanceException {
    result.map = sourceMap;
    Map<String, Object> auxMap = new HashMap<>(sourceMap);
    result.projectInodeId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.PROJECT_I_ID, ProvElasticHelper2.asLong(soft));
    result.datasetInodeId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.DATASET_I_ID, ProvElasticHelper2.asLong(soft));
    result.inodeId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.INODE_ID, ProvElasticHelper2.asLong(soft));
    result.appId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.APP_ID, ProvElasticHelper2.asString(soft));
    result.userId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.USER_ID, ProvElasticHelper2.asInt(soft));
    result.inodeName = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.INODE_NAME, ProvElasticHelper2.asString(soft));
    result.inodeOperation = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileOpsBase.INODE_OPERATION, ProvElasticHelper2.asFileOp(soft));
    result.timestamp = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileOpsBase.TIMESTAMP, ProvElasticHelper2.asLong(soft));
    result.parentInodeId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.PARENT_I_ID, ProvElasticHelper2.asLong(soft));
    result.partitionId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileAux.PARTITION_ID, ProvElasticHelper2.asLong(soft));
    result.projectName = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileAux.PROJECT_NAME, ProvElasticHelper2.asString(soft));
    result.mlId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.ML_ID, ProvElasticHelper2.asString(soft));
    result.docSubType = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.ML_TYPE, ProvElasticHelper2.asDocSubType(soft));
    result.logicalTime =ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileOpsAux.LOGICAL_TIME, ProvElasticHelper2.asInt(soft));
    result.readableTimestamp = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileOpsAux.R_TIMESTAMP, ProvElasticHelper2.asString(soft));
    ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.ENTRY_TYPE, ProvElasticHelper2.asString(soft));
    Map<String, String> xattrs = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.XAttr.XATTR_PROV, ProvElasticHelper2.asXAttrMap(true));
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
  
  public ProvElasticFields.DocSubType getDocSubType() {
    return docSubType;
  }
  
  public void setDocSubType(ProvElasticFields.DocSubType docSubType) {
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
