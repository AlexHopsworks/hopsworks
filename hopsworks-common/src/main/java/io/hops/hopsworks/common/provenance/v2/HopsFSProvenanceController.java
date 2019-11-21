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
package io.hops.hopsworks.common.provenance.v2;

import io.hops.hopsworks.common.dao.dataset.Dataset;
import io.hops.hopsworks.common.dao.featurestore.feature.FeatureDTO;
import io.hops.hopsworks.common.dao.featurestore.featuregroup.FeaturegroupDTO;
import io.hops.hopsworks.common.dao.featurestore.utils.FeaturestoreHelper;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.user.Users;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.common.hdfs.HdfsUsersController;
import io.hops.hopsworks.common.hdfs.Utils;
import io.hops.hopsworks.common.provenance.v3.xml.ProvTypeDatasetDTO;
import io.hops.hopsworks.common.provenance.v3.xml.ProvCoreDTO;
import io.hops.hopsworks.common.provenance.v3.xml.ProvFeaturesDTO;
import io.hops.hopsworks.common.provenance.v3.xml.ProvTypeDTO;
import io.hops.hopsworks.common.util.HopsworksJAXBContext;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

@Stateless(name = "HopsFSProvenanceController")
@TransactionAttribute(TransactionAttributeType.NEVER)
public class HopsFSProvenanceController {
  @EJB
  private DistributedFsService dfs;
  @EJB
  private HdfsUsersController hdfsUsersController;
  @EJB
  private Settings settings;
  @EJB
  private HopsworksJAXBContext converter;
  
  /**
   * To be used on projects/datasets - only these have a provenance core xattr
   * @param path
   * @param udfso
   * @return
   */
  private ProvCoreDTO getProvCoreXAttr(String path, DistributedFileSystemOps udfso) throws GenericException {
    byte[] provTypeB;
    try {
      provTypeB = udfso.getXAttr(path, ProvXAttrs.PROV_XATTR_CORE);
    } catch (IOException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "hopsfs - get xattr - prov core - error", "hopsfs - get xattr - prov core - error", e);
    }
    if(provTypeB == null) {
      return null;
    }
    return converter.unmarshal(new String(provTypeB), ProvCoreDTO.class);
  }
  
  private void setProvCoreXAttr(String path, ProvCoreDTO provCore, DistributedFileSystemOps udfso)
    throws GenericException {
    try {
      String provType = converter.marshal(provCore);
      udfso.upsertXAttr(path, ProvXAttrs.PROV_XATTR_CORE, provType.getBytes());
    } catch (IOException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "hopsfs - set xattr - prov core - error", "hopsfs - set xattr - prov core - error", e);
    }
  }
  
  private void setFeaturesXAttr(String path, List<FeatureDTO> features, DistributedFileSystemOps udfso)
    throws GenericException {
    ProvFeaturesDTO featuresDTO = ProvFeaturesDTO.fromFeatures(features);
    try {
      udfso.upsertXAttr(path, ProvXAttrs.PROV_XATTR_FEATURES, converter.marshal(featuresDTO).getBytes());
    } catch (IOException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "hopsfs - set xattr - prov features - error", "hopsfs - set xattr - prov features - error", e);
    }
  }
  
  public ProvCoreDTO getDatasetProvCore(Users user, Dataset dataset)
    throws GenericException {
    String hdfsUsername = hdfsUsersController.getHdfsUserName(dataset.getProject(), user);
    DistributedFileSystemOps udfso = dfs.getDfsOps(hdfsUsername);
    String datasetPath = getDatasetPath(dataset);
    try {
      return getProvCoreXAttr(datasetPath, udfso);
    } finally {
      if(udfso != null) {
        dfs.closeDfsClient(udfso);
      }
    }
  }

  public ProvTypeDTO getProjectProvType(Users user, Project project) throws GenericException {
    String hdfsUsername = hdfsUsersController.getHdfsUserName(project, user);
    DistributedFileSystemOps udfso = dfs.getDfsOps(hdfsUsername);
    String projectPath = Utils.getProjectPath(project.getName());
    try {
      ProvCoreDTO provCore = getProvCoreXAttr(projectPath, udfso);
      return provCore == null ? null : provCore.getType();
    } finally {
      if(udfso != null) {
        dfs.closeDfsClient(udfso);
      }
    }
  }
  
  public void updateProjectProvType(Users user, Project project, ProvTypeDTO provType) throws GenericException {
    String hdfsUsername = hdfsUsersController.getHdfsUserName(project, user);
    DistributedFileSystemOps udfso = dfs.getDfsOps(hdfsUsername);
    try {
      updateProjectProvType(project, provType, udfso);
    } finally {
      if(udfso != null) {
        dfs.closeDfsClient(udfso);
      }
    }
  }
  
  public void updateProjectProvType(Project project, ProvTypeDTO newProvType, DistributedFileSystemOps dfso)
    throws GenericException {
    String projectPath = Utils.getProjectPath(project.getName());
    
    ProvCoreDTO provCore = getProvCoreXAttr(projectPath, dfso);
    if (provCore != null && newProvType.equals(provCore.getType())) {
      return;
    }
    provCore = new ProvCoreDTO(newProvType, null);
    setProvCoreXAttr(projectPath, provCore, dfso);
  
    provCore = new ProvCoreDTO(newProvType, project.getInode().getId());
    for (Dataset dataset : project.getDatasetCollection()) {
      String datasetPath = getDatasetPath(dataset);
      ProvCoreDTO datasetProvCore = getProvCoreXAttr(datasetPath, dfso);
      if(datasetProvCore != null
        && (datasetProvCore.getType().equals(ProvTypeDTO.ProvType.DISABLED.dto)
          || datasetProvCore.getType().equals(newProvType))) {
        continue;
      }
      updateDatasetProvType(datasetPath, provCore, dfso);
    }
  }
  
  public void updateDatasetProvType(Dataset dataset, ProvTypeDTO newProvType, DistributedFileSystemOps dfso)
    throws GenericException {
    ProvCoreDTO newProvCore = new ProvCoreDTO(newProvType, dataset.getProject().getInode().getId());
    String datasetPath = getDatasetPath(dataset);
    ProvCoreDTO currentProvCore = getProvCoreXAttr(datasetPath, dfso);
    if(currentProvCore != null && currentProvCore.getType().equals(newProvType)) {
      return;
    }
    updateDatasetProvType(datasetPath, newProvCore, dfso);
  }
  
  public void newHiveDatasetProvCore(Project project, String hiveDBPath, DistributedFileSystemOps dfso)
    throws GenericException {
    String projectPath = Utils.getProjectPath(project.getName());
    ProvCoreDTO projectProvCore = getProvCoreXAttr(projectPath, dfso);
    if(projectProvCore == null) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "hopsfs - hive db - set meta status error - project without prov core");
    }
    ProvCoreDTO datasetProvCore = new ProvCoreDTO(projectProvCore.getType(), project.getInode().getId());
    updateDatasetProvType(hiveDBPath, datasetProvCore, dfso);
  }
  
  private void updateDatasetProvType(String datasetPath, ProvCoreDTO provCore, DistributedFileSystemOps dfso)
    throws GenericException {
    try {
      dfso.setMetaStatus(datasetPath, provCore.getType().getMetaStatus());
    } catch (IOException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "hopsfs - dataset set meta status error", "hopsfs - dataset set meta status error", e);
    }
    setProvCoreXAttr(datasetPath, provCore, dfso);
  }
  
  public List<ProvTypeDatasetDTO> getDatasetsProvType(Users user, Project project) throws GenericException {
    String hdfsUsername = hdfsUsersController.getHdfsUserName(project, user);
    DistributedFileSystemOps udfso = dfs.getDfsOps(hdfsUsername);
    
    try {
      List<ProvTypeDatasetDTO> result = new ArrayList<>();
      for (Dataset dataset : project.getDatasetCollection()) {
        String datasetPath = getDatasetPath(dataset);
        ProvCoreDTO provCore = getProvCoreXAttr(datasetPath, udfso);
        ProvTypeDatasetDTO dsState = new ProvTypeDatasetDTO(dataset.getName(), dataset.getInode().getId(),
          provCore.getType());
        result.add(dsState);
      }
      return result;
    } finally {
      if (udfso != null) {
        udfso.close();
      }
    }
  }
  
  private String getDatasetPath(Dataset dataset) {
    if(dataset.getName().equals(FeaturestoreHelper.getHiveDBName(dataset.getProject()))) {
      return FeaturestoreHelper.getHiveDBPath(settings, dataset.getProject());
    } else if(dataset.getName().equals(FeaturestoreHelper.getFeaturestoreName(dataset.getProject()))) {
      return FeaturestoreHelper.getFeaturestorePath(settings, dataset.getProject());
    } else {
      return Utils.getDatasetPath(dataset.getProject().getName(), dataset.getName());
    }
  }
  
  public void featuregroupAttachXAttrs(Users user, Project project, FeaturegroupDTO featuregroup)
    throws GenericException {
    String hdfsUsername = hdfsUsersController.getHdfsUserName(project, user);
    DistributedFileSystemOps udfso = dfs.getDfsOps(hdfsUsername);
    
    try {
      String featuregroupPath = FeaturestoreHelper.getFeaturestorePath(settings, project)
        + "/" + FeaturestoreHelper.getFeaturegroupName(featuregroup.getName(), featuregroup.getVersion());
      setFeaturesXAttr(featuregroupPath, featuregroup.getFeatures(), udfso);
    } finally {
      if(udfso != null) {
        dfs.closeDfsClient(udfso);
      }
    }
  }
  
  public void trainingDatasetAttachXAttr(Users user, Project project, String path, List<FeatureDTO> features)
    throws GenericException {
    String hdfsUsername = hdfsUsersController.getHdfsUserName(project, user);
    DistributedFileSystemOps udfso = dfs.getDfsOps(hdfsUsername);
    try {
      setFeaturesXAttr(path, features, udfso);
    } finally {
      if(udfso != null) {
        dfs.closeDfsClient(udfso);
      }
    }
  }
}
