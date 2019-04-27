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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.hops.hopsworks.common.provenance.MLAssetAppState;
import io.hops.hopsworks.common.provenance.v2.ProvElasticFields;
import io.hops.hopsworks.common.provenance.v2.ProvHelper;
import io.hops.hopsworks.exceptions.GenericException;
import org.elasticsearch.search.SearchHit;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

@XmlRootElement
public class FileOp implements Comparator<FileOp>  {
  private static final Logger LOG = Logger.getLogger(FileOp.class.getName());
  public static final TimestampComparator timestampComparator = new TimestampComparator();
  
  private String id;
  private float score;
  private Map<String, Object> map;
  
  private Long inodeId;
  private String inodeOperation;
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
  private String mlType;
  private String mlId;
  private MLAssetAppState appState;
  
  public FileOp() {}
  
  public static FileOp instance(SearchHit hit, boolean soft) throws GenericException {
    FileOp result = new FileOp();
    result.id = hit.getId();
    result.score = Float.isNaN(hit.getScore()) ? 0 : hit.getScore();
    return instance(result, hit.getSourceAsMap(), soft);
  }
  
  public static FileOp instance(String id, Map<String, Object> sourceMap, boolean soft) throws GenericException {
    FileOp result = new FileOp();
    result.id = id;
    result.score = 0;
    return instance(result, sourceMap, soft);
  }
  
  private static FileOp instance(FileOp result, Map<String, Object> sourceMap, boolean soft) throws GenericException {
    result.map = sourceMap;
    Map<String, Object> auxMap = new HashMap<>(sourceMap);
    result.projectInodeId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.PROJECT_I_ID, ProvHelper.asLong(soft));
    result.datasetInodeId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.DATASET_I_ID, ProvHelper.asLong(soft));
    result.inodeId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.INODE_ID, ProvHelper.asLong(soft));
    result.appId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.APP_ID, ProvHelper.asString(soft));
    result.userId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.USER_ID, ProvHelper.asInt(soft));
    result.inodeName = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.INODE_NAME, ProvHelper.asString(soft));
    result.inodeOperation = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileOpsBase.INODE_OPERATION, ProvHelper.asString(soft));
    result.timestamp = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileOpsBase.TIMESTAMP, ProvHelper.asLong(soft));
    result.parentInodeId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.PARENT_I_ID, ProvHelper.asLong(soft));
    result.partitionId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileAux.PARTITION_ID, ProvHelper.asLong(soft));
    result.projectName = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileAux.PROJECT_NAME, ProvHelper.asString(soft));
    result.mlId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileOpsAux.ML_ID, ProvHelper.asString(soft));
    result.mlType = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileOpsAux.ML_TYPE, ProvHelper.asString(soft));
    result.logicalTime =ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileOpsAux.LOGICAL_TIME, ProvHelper.asInt(soft));
    result.readableTimestamp = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileOpsAux.R_TIMESTAMP, ProvHelper.asString(soft));
    ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.ENTRY_TYPE, ProvHelper.asString(soft));
    Map<String, String> xattrs = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.XAttr.XATTR_PROV, ProvHelper.asXAttrMap(true));
    if(xattrs != null && xattrs.size() == 1) {
      Map.Entry<String, String> e = xattrs.entrySet().iterator().next();
      result.xattrName = e.getKey();
      result.xattrVal = e.getValue();
    }
    return result;
  }
  
  @Override
  public int compare(FileOp o1, FileOp o2) {
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
  
  public String getInodeOperation() {
    return inodeOperation;
  }
  
  public void setInodeOperation(String inodeOperation) {
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
  
  public MLAssetAppState getAppState() {
    return appState;
  }
  
  public void setAppState(MLAssetAppState appState) {
    this.appState = appState;
  }
  
  public String getMlType() {
    return mlType;
  }
  
  public void setMlType(String mlType) {
    this.mlType = mlType;
  }
  
  public String getMlId() {
    return mlId;
  }
  
  public void setMlId(String mlId) {
    this.mlId = mlId;
  }
  
  public static class TimestampComparator implements Comparator<FileOp> {
  
    @Override
    public int compare(FileOp o1, FileOp o2) {
      int result = Ints.compare(o1.logicalTime, o2.logicalTime);
      if(result == 0) {
        result = Longs.compare(o1.timestamp, o2.timestamp);
      }
      return result;
    }
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
