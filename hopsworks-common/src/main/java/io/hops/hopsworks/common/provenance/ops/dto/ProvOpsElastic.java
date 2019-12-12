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
package io.hops.hopsworks.common.provenance.ops.dto;

import io.hops.hopsworks.common.provenance.app.dto.ProvAppStateDTO;
import io.hops.hopsworks.common.provenance.core.ProvParser;
import io.hops.hopsworks.common.provenance.core.elastic.BasicElasticHit;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.util.ProvHelper;
import io.hops.hopsworks.common.provenance.ops.apiToElastic.ProvOParser;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.restutils.RESTCodes;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

@XmlRootElement
public class ProvOpsElastic implements Comparator<ProvOpsElastic>  {
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
  private ProvAppStateDTO appState;
  
  public ProvOpsElastic() {}
  
  public static ProvOpsElastic instance(BasicElasticHit hit) throws ProvenanceException {
    ProvOpsElastic result = new ProvOpsElastic();
    result.id = hit.getId();
    result.score = Float.isNaN(hit.getScore()) ? 0 : hit.getScore();
    result.map = hit.getSource();
    return instance(result);
  }
  
  private static ProvOpsElastic instance(ProvOpsElastic result)
    throws ProvenanceException {
    Map<String, Object> auxMap = new HashMap<>(result.map);
    try {
      result.projectInodeId = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.PROJECT_I_ID);
      result.datasetInodeId = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.DATASET_I_ID);
      result.inodeId = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.FILE_I_ID);
      result.appId = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.APP_ID);
      result.userId = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.USER_ID);
      result.inodeName = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.FILE_NAME);
      result.inodeOperation = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.FILE_OPERATION);
      result.timestamp = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.TIMESTAMP);
      result.parentInodeId = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.PARENT_I_ID);
      result.partitionId = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.PARTITION_ID);
      result.projectName = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.PROJECT_NAME);
      result.mlId = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.ML_ID);
      result.docSubType = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.DOC_SUBTYPE);
      result.logicalTime = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.LOGICAL_TIME);
      result.readableTimestamp = ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.R_TIMESTAMP);
      ProvHelper.extractElasticField(auxMap, ProvOParser.Fields.ENTRY_TYPE);
      Map<String, String> xattrs = ProvHelper.extractElasticField(auxMap,
        ProvParser.XAttrField.XATTR_PROV, ProvHelper.asXAttrMap(), true);
      if(xattrs != null && xattrs.size() == 1) {
        Map.Entry<String, String> e = xattrs.entrySet().iterator().next();
        result.xattrName = e.getKey();
        result.xattrVal = e.getValue();
      }
    } catch(ClassCastException e) {
      String msg = "mistmatch between DTO class and ProvOParser field types (elastic)";
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.INTERNAL_ERROR, Level.WARNING, msg, msg, e);
    }
    return result;
  }
  
  @Override
  public int compare(ProvOpsElastic o1, ProvOpsElastic o2) {
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
  
  public ProvAppStateDTO getAppState() {
    return appState;
  }
  
  public void setAppState(ProvAppStateDTO appState) {
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
