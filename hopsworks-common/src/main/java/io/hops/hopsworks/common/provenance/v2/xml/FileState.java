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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.annotation.XmlRootElement;

import io.hops.hopsworks.common.provenance.MLAssetAppState;
import io.hops.hopsworks.common.provenance.v2.ProvElasticFields;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.v2.ProvHelper;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.search.SearchHit;

@XmlRootElement
public class FileState implements Comparator<FileState>, ProvenanceController.BasicFileState {
  
  private static final Logger LOG = Logger.getLogger(FileState.class.getName());
  private String id;
  private float score;
  private Map<String, Object> map;
  
  private Long inodeId;
  private String appId;
  private Integer userId;
  private Long projectInodeId;
  private Long datasetInodeId;
  private String inodeName;
  private String projectName;
  private String mlType;
  private String mlId;
  private Long createTime;
  private String readableCreateTime;
  private Map<String, String> xattrs = new HashMap<>();
  private MLAssetAppState appState;
  private String fullPath;
  private Long partitionId;
  private Long parentInodeId;
  
  public static FileState instance(SearchHit hit, boolean tlsEnabled) throws GenericException {
    FileState result = new FileState();
    result.id = hit.getId();
    result.score = Float.isNaN(hit.getScore()) ? 0 : hit.getScore();
    return instance(result, hit.getSourceAsMap(), tlsEnabled);
  }
  
  public static FileState instance(String id, Map<String, Object> sourceMap, boolean tlsEnabled)
    throws GenericException {
    FileState result = new FileState();
    result.id = id;
    result.score = 0;
    return instance(result, sourceMap, tlsEnabled);
  }
  
  private static FileState instance(FileState result, Map<String, Object> sourceMap, boolean tlsEnabled)
    throws GenericException {
    result.map = sourceMap;
    Map<String, Object> auxMap = new HashMap<>(sourceMap);
    result.projectInodeId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.PROJECT_I_ID, ProvHelper.asLong(false));
    result.inodeId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.INODE_ID, ProvHelper.asLong(false));
    result.appId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.APP_ID, ProvHelper.asString(false));
    result.userId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.USER_ID, ProvHelper.asInt(false));
    result.inodeName = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.INODE_NAME, ProvHelper.asString(false));
    result.createTime = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileStateBase.CREATE_TIMESTAMP, ProvHelper.asLong(false));
    result.mlType = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileStateBase.ML_TYPE, ProvHelper.asString(false));
    result.mlId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileStateBase.ML_ID, ProvHelper.asString(false));
    result.datasetInodeId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.DATASET_I_ID, ProvHelper.asLong(false));
    result.parentInodeId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.PARENT_I_ID, ProvHelper.asLong(false));
    result.partitionId = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileAux.PARTITION_ID, ProvHelper.asLong(false));
    result.projectName = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileAux.PROJECT_NAME, ProvHelper.asString(false));
    result.readableCreateTime = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileStateAux.R_CREATE_TIMESTAMP, ProvHelper.asString(false));
    ProvElasticFields.extractField(auxMap,
      ProvElasticFields.FileBase.ENTRY_TYPE, ProvHelper.asString(false));
    result.xattrs = ProvElasticFields.extractField(auxMap,
      ProvElasticFields.XAttr.XATTR_PROV, ProvHelper.asXAttrMap(true));
    if(!tlsEnabled) {
      if(result.xattrs != null && result.xattrs.containsKey(ProvElasticFields.FileBase.APP_ID.toString())) {
        result.appId = ProvHelper.asString(false)
          .apply(result.xattrs.get(ProvElasticFields.FileBase.APP_ID.toString()));
      }
    }
    for (Map.Entry<String, Object> entry : auxMap.entrySet()) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "field:" + entry.getKey() + "not managed in file state return");
    }
    return result;
  }

  public float getScore() {
    return score;
  }
  
  @Override
  public int compare(FileState o1, FileState o2) {
    return Float.compare(o2.getScore(), o1.getScore());
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
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

  @Override
  public Long getInodeId() {
    return inodeId;
  }

  public void setInodeId(long inodeId) {
    this.inodeId = inodeId;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public int getUserId() {
    return userId;
  }

  public void setUserId(int userId) {
    this.userId = userId;
  }

  @Override
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
  
  @Override
  public String getInodeName() {
    return inodeName;
  }

  public void setInodeName(String inodeName) {
    this.inodeName = inodeName;
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

  public Long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Long createTime) {
    this.createTime = createTime;
  }

  public String getReadableCreateTime() {
    return readableCreateTime;
  }

  public void setReadableCreateTime(String readableCreateTime) {
    this.readableCreateTime = readableCreateTime;
  }

  public Map<String, String> getXattrs() {
    return xattrs;
  }

  public void setXattrs(Map<String, String> xattrs) {
    this.xattrs = xattrs;
  }

  public MLAssetAppState getAppState() {
    return appState;
  }

  public void setAppState(MLAssetAppState appState) {
    this.appState = appState;
  }
  
  public String getFullPath() {
    return fullPath;
  }
  
  public void setFullPath(String fullPath) {
    this.fullPath = fullPath;
  }
  
  public Long getPartitionId() {
    return partitionId;
  }
  
  public void setPartitionId(Long partitionId) {
    this.partitionId = partitionId;
  }
  
  @Override
  public Long getParentInodeId() {
    return parentInodeId;
  }
  
  public void setParentInodeId(Long parentInodeId) {
    this.parentInodeId = parentInodeId;
  }
  
  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }
  
  public void setUserId(Integer userId) {
    this.userId = userId;
  }
  
  public String getProjectName() {
    return projectName;
  }
  
  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }
  
  @Override
  public boolean isProject() {
    return projectInodeId == inodeId;
  }
}
