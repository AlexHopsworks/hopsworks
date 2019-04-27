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

import io.hops.hopsworks.common.elastic.ProvElasticHelper;
import io.hops.hopsworks.common.provenance.v2.ProvElasticFields;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.javatuples.Pair;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

public class FileOpDTO {
  @XmlRootElement
  public static class Base {
    private Long inodeId;
    private String inodeOperation;
    private String appId;
    private Integer userId;
    private Long parentInodeId;
    private Long projectInodeId;
    private Integer logicalTime;
    private Long timestamp;
    private String inodeName;
    private String xattrName;
    
    public Base() {}
    
    public Base(FileOp op) {
      this.inodeId = op.getInodeId();
      this.inodeOperation = op.getInodeOperation();
      this.appId = op.getAppId();
      this.userId = op.getUserId();
      this.parentInodeId = op.getParentInodeId();
      this.projectInodeId = op.getProjectInodeId();
      this.logicalTime = op.getLogicalTime();
      this.timestamp = op.getTimestamp();
      this.inodeName = op.getInodeName();
      this.xattrName = op.getXattrName();
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
  
    public Integer getUserId() {
      return userId;
    }
  
    public void setUserId(Integer userId) {
      this.userId = userId;
    }
  
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
  }
  
  @XmlRootElement
  public static class CompactByFile {
    private String inodeOperation;
    private String appId;
    private Integer userId;
    private Integer logicalTime;
    private Long timestamp;
    private String xattrName;
    
    public CompactByFile() {}
    
    public CompactByFile(Base op) {
      this.inodeOperation = op.inodeOperation;
      this.appId = op.appId;
      this.userId = op.userId;
      this.logicalTime = op.logicalTime;
      this.timestamp = op.timestamp;
      this.xattrName = op.xattrName;
    }
    
    public CompactByFile(FileOp op) {
      this.inodeOperation = op.getInodeOperation();
      this.appId = op.getAppId();
      this.userId = op.getUserId();
      this.logicalTime = op.getLogicalTime();
      this.timestamp = op.getTimestamp();
      this.xattrName = op.getXattrName();
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
  
    public Integer getUserId() {
      return userId;
    }
  
    public void setUserId(Integer userId) {
      this.userId = userId;
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
  
    public String getXattrName() {
      return xattrName;
    }
  
    public void setXattrName(String xattrName) {
      this.xattrName = xattrName;
    }
  }
  @XmlRootElement
  public static class File {
    private Long inodeId;
    private Long parentInodeId;
    private Long projectInodeId;
    private String inodeName;
    private List<CompactByFile> ops = new LinkedList<>();
    
    public File(){}
    
    public File(Base op){
      this.inodeId = op.inodeId;
      this.parentInodeId = op.parentInodeId;
      this.projectInodeId = op.projectInodeId;
      this.inodeName = op.inodeName;
      ops.add(new CompactByFile(op));
    }
  
    public File(FileOp op){
      this.inodeId = op.getInodeId();
      this.parentInodeId = op.getParentInodeId();
      this.projectInodeId = op.getProjectInodeId();
      this.inodeName = op.getInodeName();
      ops.add(new CompactByFile(op));
    }
  
    public Long getInodeId() {
      return inodeId;
    }
  
    public void setInodeId(Long inodeId) {
      this.inodeId = inodeId;
    }
  
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
  
    public String getInodeName() {
      return inodeName;
    }
  
    public void setInodeName(String inodeName) {
      this.inodeName = inodeName;
    }
  
    public List<CompactByFile> getOps() {
      return ops;
    }
  
    public void setOps(List<CompactByFile> ops) {
      this.ops = ops;
    }
    
    public void addOp(Base op) throws GenericException {
      if(!op.getInodeId().equals(this.inodeId)) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "op mismatch - inodeId");
      }
      ops.add(new CompactByFile(op));
    }
  
    public void addOp(FileOp op) throws GenericException {
      if(!op.getInodeId().equals(this.inodeId)) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "op mismatch - inodeId");
      }
      ops.add(new CompactByFile(op));
    }
    
    public static File compact(List<FileOp> ops) throws GenericException {
      if(ops.isEmpty()) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "cannot create a FileOps object out of no ops");
      }
      Iterator<FileOp> it = ops.iterator();
      File fileOps = new File(it.next());
      while(it.hasNext()) {
        fileOps.addOp(it.next());
      }
      return fileOps;
    }
  }
  
  @XmlRootElement
  public static class PList {
    List<FileOp> items;
    Long count;
    
    public PList() {}
    
    public PList(List<FileOp> items, Long count) {
      this.items = items;
      this.count = count;
    }
    
    public List<FileOp> getItems() {
      return items;
    }
    
    public void setItems(List<FileOp> items) {
      this.items = items;
    }
  
    public Long getCount() {
      return count;
    }
  
    public void setCount(Long count) {
      this.count = count;
    }
  }
  
  @XmlRootElement
  public static class CountAux {
    Long count;
    List<FileAggregation> items;
  
    public CountAux() {}
  
    public CountAux(List<FileAggregation> items) {
      this.count = (long)items.size();
      this.items = items;
    }
  
    public Long getCount() {
      return count;
    }
  
    public void setCount(Long count) {
      this.count = count;
    }
  
    public List<FileAggregation> getItems() {
      return items;
    }
  
    public void setItems(List<FileAggregation> items) {
      this.items = items;
    }
  }
  
  @XmlRootElement
  public static class Count {
    Long count;
    CountAux filesInProject = null;
    CountAux filesLeastAccessedByLastAccessed = null;
    CountAux projectLeastAccessedByLastAccessed = null;
    CountAux artifactFootprint = null;
  
    public Count() {}
    
    public Count(Long count,
      CountAux filesInProject,
      CountAux filesLeastAccessedByLastAccessed,
      CountAux projectLeastAccessedByLastAccessed,
      CountAux artifactFootprint) {
      this.count = count;
      this.filesInProject = filesInProject;
      this.filesLeastAccessedByLastAccessed = filesLeastAccessedByLastAccessed;
      this.projectLeastAccessedByLastAccessed = projectLeastAccessedByLastAccessed;
      this.artifactFootprint = artifactFootprint;
    }
    
    public static Count instance(Pair<Long, List<Pair<ProvElasticHelper.ProvAggregations, List>>> elasticResult)
      throws GenericException {
      CountAux filesInProject = null;
      CountAux filesLeastAccessedByLastAccessed = null;
      CountAux projectLeastAccessedByLastAccessed = null;
      CountAux artifactFootprint = null;
      
      for(Pair<ProvElasticHelper.ProvAggregations, List> aggregation : elasticResult.getValue1()) {
        switch(aggregation.getValue0()) {
          case FILES_IN:
            filesInProject = new CountAux(aggregation.getValue1());
            break;
          case FILES_LEAST_ACTIVE_BY_LAST_ACCESSED:
            filesLeastAccessedByLastAccessed = new CountAux(aggregation.getValue1());
            break;
          case PROJECTS_LEAST_ACTIVE_BY_LAST_ACCESSED:
            projectLeastAccessedByLastAccessed = new CountAux(aggregation.getValue1());
            break;
          case ARTIFACT_FOOTPRINT:
            artifactFootprint = new CountAux(aggregation.getValue1());
            break;
          default:
            throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
              "aggregation:" + aggregation.getValue0().toString() + " not handled");
        }
      }
      return new Count(elasticResult.getValue0(), filesInProject, filesLeastAccessedByLastAccessed,
        projectLeastAccessedByLastAccessed, artifactFootprint);
    }
    
    public Long getCount() {
      return count;
    }
  
    public void setCount(Long count) {
      this.count = count;
    }
  
    public CountAux getFilesInProject() {
      return filesInProject;
    }
  
    public void setFilesInProject(CountAux filesInProject) {
      this.filesInProject = filesInProject;
    }
  
    public CountAux getFilesLeastAccessedByLastAccessed() {
      return filesLeastAccessedByLastAccessed;
    }
  
    public void setFilesLeastAccessedByLastAccessed(CountAux filesLeastAccessedByLastAccessed) {
      this.filesLeastAccessedByLastAccessed = filesLeastAccessedByLastAccessed;
    }
  
    public CountAux getProjectLeastAccessedByLastAccessed() {
      return projectLeastAccessedByLastAccessed;
    }
  
    public void setProjectLeastAccessedByLastAccessed(CountAux projectLeastAccessedByLastAccessed) {
      this.projectLeastAccessedByLastAccessed = projectLeastAccessedByLastAccessed;
    }
  
    public CountAux getArtifactFootprint() {
      return artifactFootprint;
    }
  
    public void setArtifactFootprint(CountAux artifactFootprint) {
      this.artifactFootprint = artifactFootprint;
    }
  }
  
  @XmlRootElement
  public static class FileAggregation {
    Long inodeId;
    Long count;
    Long lastAccessed;
    
    public FileAggregation() {}
  
    public FileAggregation(Long inodeId, Long count) {
      this.inodeId = inodeId;
      this.count = count;
    }
    
    public FileAggregation(Long inodeId, Long count, Long lastAccessed) {
      this.inodeId = inodeId;
      this.count = count;
      this.lastAccessed = lastAccessed;
    }
  
    public Long getInodeId() {
      return inodeId;
    }
  
    public void setInodeId(Long inodeId) {
      this.inodeId = inodeId;
    }
  
    public Long getCount() {
      return count;
    }
  
    public void setCount(Long count) {
      this.count = count;
    }
  
    public Long getLastAccessed() {
      return lastAccessed;
    }
  
    public void setLastAccessed(Long lastAccessed) {
      this.lastAccessed = lastAccessed;
    }
  }
  
  public enum ArtifactOp {
    CREATE,
    DELETE,
    READ,
    MODIFY
  }
  
  @XmlRootElement
  public static class ArtifactFile {
    private Long parentId;
    private Long inodeId;
    private Base createOp;
    private Base deleteOp;
    private Base firstReadOp;
    private Long readCount;
    private Base firstAppendOp;
    private Long appendCount;
  
    public ArtifactFile() {
    }
  
    public void addCreate(FileOp createOp) {
      init(createOp);
      this.createOp = new Base(createOp);
    }
  
    public void addDelete(FileOp deleteOp) {
      init(deleteOp);
      this.deleteOp = new Base(deleteOp);
    }
  
    public void addFirstRead(FileOp readOp, Long readCount) {
      init(readOp);
      this.firstReadOp = new Base(readOp);
      this.readCount = readCount;
    }
    public void addFirstAppend(FileOp appendOp, Long appendCount) {
      init(appendOp);
      this.firstAppendOp = new Base(appendOp);
      this.appendCount = appendCount;
    }
    
    private void init(FileOp op) {
      if(parentId == null) {
        parentId = op.getParentInodeId();
      }
      if(inodeId == null) {
        inodeId = op.getInodeId();
      }
    }
  
    public Long getParentId() {
      return parentId;
    }
  
    public void setParentId(Long parentId) {
      this.parentId = parentId;
    }
  
    public Long getInodeId() {
      return inodeId;
    }
  
    public void setInodeId(Long inodeId) {
      this.inodeId = inodeId;
    }
  
    public Base getCreateOp() {
      return createOp;
    }
  
    public void setCreateOp(Base createOp) {
      this.createOp = createOp;
    }
  
    public Base getDeleteOp() {
      return deleteOp;
    }
  
    public void setDeleteOp(Base deleteOp) {
      this.deleteOp = deleteOp;
    }
  
    public Base getFirstReadOp() {
      return firstReadOp;
    }
  
    public void setFirstReadOp(Base firstReadOp) {
      this.firstReadOp = firstReadOp;
    }
  
    public Base getFirstAppendOp() {
      return firstAppendOp;
    }
  
    public void setFirstAppendOp(Base firstAppendOp) {
      this.firstAppendOp = firstAppendOp;
    }
  
    public Long getReadOps() {
      return readCount;
    }
  
    public void setReadOps(Long readOps) {
      this.readCount = readOps;
    }
  
    public Long getReadCount() {
      return readCount;
    }
  
    public void setReadCount(Long readCount) {
      this.readCount = readCount;
    }
  
    public Long getAppendCount() {
      return appendCount;
    }
  
    public void setAppendCount(Long appendCount) {
      this.appendCount = appendCount;
    }
  }
  
  @XmlRootElement
  public static class ArtifactBase {
    private Long projectInodeId;
    private Long datasetInodeId;
    private String mlId;
    private ProvElasticFields.MLType mlType;
  
    public ArtifactBase() {
    }
  
    public ArtifactBase(Long projectInodeId, Long datasetInodeId, String mlId,
      ProvElasticFields.MLType mlType) {
      this.projectInodeId = projectInodeId;
      this.datasetInodeId = datasetInodeId;
      this.mlId = mlId;
      this.mlType = mlType;
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
  
    public String getMlId() {
      return mlId;
    }
  
    public void setMlId(String mlId) {
      this.mlId = mlId;
    }
  
    public ProvElasticFields.MLType getMlType() {
      return mlType;
    }
  
    public void setMlType(ProvElasticFields.MLType mlType) {
      this.mlType = mlType;
    }
  }
  
  @XmlRootElement
  public static class Artifact {
    private ArtifactBase base;
    private List<ArtifactFile> components = new LinkedList<>();
  
    public Artifact() {}
    
    public void addComponent(ArtifactFile file) {
      components.add(file);
    }
  
    public ArtifactBase getBase() {
      return base;
    }
  
    public void setBase(ArtifactBase base) {
      this.base = base;
    }
  
    public List<ArtifactFile> getComponents() {
      return components;
    }
  
    public void setComponents(List<ArtifactFile> components) {
      this.components = components;
    }
  }
  
  @XmlRootElement
  public static class DatasetArtifacts {
    private Long projectInodeId;
    private Long datasetInodeId;
    private List<Artifact> artifacts = new LinkedList<>();
  
    public DatasetArtifacts() {}
  
    public DatasetArtifacts(Long projectInodeId, Long datasetInodeId, List<Artifact> artifacts) {
      this.projectInodeId = projectInodeId;
      this.datasetInodeId = datasetInodeId;
      this.artifacts = artifacts;
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
  
    public List<Artifact> getArtifacts() {
      return artifacts;
    }
  
    public void setArtifacts(List<Artifact> artifacts) {
      this.artifacts = artifacts;
    }
  }
}
