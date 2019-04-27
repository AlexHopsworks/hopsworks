/*
 * Changes to this file committed after and not including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
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
 *
 * Changes to this file committed before and including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
 * Copyright (C) 2013 - 2018, Logical Clocks AB and RISE SICS AB. All rights reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS  OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package io.hops.hopsworks.common.provenance;

import io.hops.hopsworks.common.dao.hdfs.inode.Inode;
import io.hops.hopsworks.common.dao.hdfs.inode.InodeFacade;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.elastic.ProvElasticController;
import io.hops.hopsworks.common.elastic.ProvElasticHelper;
import io.hops.hopsworks.common.hdfs.Utils;
import io.hops.hopsworks.common.provenance.v2.ProvElasticFields;
import io.hops.hopsworks.common.provenance.v2.ProvFileOps;
import io.hops.hopsworks.common.provenance.v2.ProvFileOpsParamBuilder;
import io.hops.hopsworks.common.provenance.v2.ProvFileQuery;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.v2.xml.ArchiveDTO;
import io.hops.hopsworks.common.provenance.v2.xml.FileOp;
import io.hops.hopsworks.common.provenance.v2.xml.FileOpDTO;
import io.hops.hopsworks.common.provenance.v2.xml.FileState;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateDTO;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateTree;
import io.hops.hopsworks.common.provenance.v2.xml.FootprintFileState;
import io.hops.hopsworks.common.provenance.v2.xml.FootprintFileStateTree;
import io.hops.hopsworks.common.provenance.v2.xml.TreeHelper;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProvenanceController {
  private final static Logger LOGGER = Logger.getLogger(ProvenanceController.class.getName());
  @EJB
  private ProvElasticController elasticCtrl;
  @EJB
  private InodeFacade inodeFacade;
  @EJB
  private ProjectFacade projectFacade;
  @EJB
  private Settings settings;
  
  public Map<String, String> mngIndexGetMapping(String indexRegex, boolean forceFetch) throws ServiceException {
    return elasticCtrl.mngIndexGetMapping(indexRegex, forceFetch);
  }
  
//  public void createIndex(Project project) throws ServiceException, GenericException {
//    elasticCtrl.createProvIndex(project.getInode().getId());
//  }
//
//  public void deleteIndex(Project project) throws ServiceException {
//    elasticCtrl.deleteProvIndex(project.getInode().getId());
//  }
//
//  public String[] getAllIndices() {
//    return elasticCtrl.getAllIndices();
//  }
  
  public FileStateDTO.PList provFileStateList(Project project, ProvFileStateParamBuilder params)
    throws GenericException, ServiceException {
    if(params.getPagination() != null && !params.getAppStateFilter().isEmpty()) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "cannot use pagination with app state filtering");
    }
  
    checkMapping(project, params);
    FileStateDTO.PList fileStates = elasticCtrl.provFileState(project.getInode().getId(),
      params.getFileStateFilter(), params.getFileStateSortBy(),
      params.getExactXAttrFilter(), params.getLikeXAttrFilter(), params.getHasXAttrFilter(),
      params.getXAttrSortBy(), params.getPagination().getValue0(), params.getPagination().getValue1());

    if (params.hasAppExpansion()) {
      //If withAppStates, update params based on appIds of items files and do a appState index query.
      //After this filter the fileStates based on the results of the appState query
      for (FileState fileState : fileStates.getItems()) {
        Optional<String> appId = getAppId(fileState);
        if(appId.isPresent()) {
          params.withAppExpansion(appId.get());
        }
      }
      Map<String, Map<Provenance.AppState, AppProvenanceHit>> appExps
        = elasticCtrl.provAppState(params.getAppStateFilter());
      Iterator<FileState> fileStateIt = fileStates.getItems().iterator();
      while(fileStateIt.hasNext()) {
        FileState fileState = fileStateIt.next();
        Optional<String> appId = getAppId(fileState);
        if(appId.isPresent() && appExps.containsKey(appId.get())) {
          Map<Provenance.AppState, AppProvenanceHit> appExp = appExps.get(appId.get());
          fileState.setAppState(buildAppState(appExp));
        } else {
          fileState.setAppState(MLAssetAppState.unknown());
        }
      }
    }
    return fileStates;
  }
  
  private void checkMapping(Project project, ProvFileStateParamBuilder params)
    throws GenericException, ServiceException {
    String index = Provenance.getProjectIndex(project);
    Map<String, String> mapping = elasticCtrl.mngIndexGetMapping(index, false);
    if(mapping == null) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "provenance file state - no index");
    }
    try {
      params.fixSortBy(index, mapping);
    } catch(ServiceException e) {
      mapping = elasticCtrl.mngIndexGetMapping(index, true);
      params.fixSortBy(index, mapping);
    }
  }
  
  public Pair<Map<Long, FileStateTree>, Map<Long, FileStateTree>> provFileStateTree(Project project,
    ProvFileStateParamBuilder params, boolean fullTree)
    throws GenericException, ServiceException {
    List<FileState> fileStates = provFileStateList(project, params).getItems();
    Pair<Map<Long, ProvenanceController.BasicTreeBuilder<FileState>>,
      Map<Long, ProvenanceController.BasicTreeBuilder<FileState>>> result
      = processAsTree(project, fileStates, () -> new FileStateTree(), fullTree);
    return Pair.with((Map<Long, FileStateTree>)(Map)result.getValue0(),
      (Map<Long, FileStateTree>)(Map)(result.getValue1()));
  }
  
  public long provFileStateCount(Project project, ProvFileStateParamBuilder params)
    throws GenericException, ServiceException {
    if(params.hasAppExpansion()) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "provenance file state count does not currently work with app state expansion");
    }
    return elasticCtrl.provFileStateCount(project.getInode().getId(), params.getFileStateFilter(),
      params.getExactXAttrFilter(), params.getLikeXAttrFilter(), params.getHasXAttrFilter());
  }
  
  public FileOpDTO.PList provFileOpsList(Project project, ProvFileOpsParamBuilder params)
    throws GenericException, ServiceException {
    if(!params.getAggregations().isEmpty()) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "aggregations currently only allowed with count");
    }
    FileOpDTO.PList fileOps;
    if(params.hasPagination()) {
      fileOps =  elasticCtrl.provFileOpsBase(project.getInode().getId(),
        params.getFileOpsFilterBy(), params.getFilterScripts(), params.getFileOpsSortBy(),
        params.getPagination().getValue0(), params.getPagination().getValue1(), false);
    } else {
      fileOps =  elasticCtrl.provFileOpsScrolling(project.getInode().getId(), params.getFileOpsFilterBy(),
        params.getFilterScripts(), false);
    }
  
    if (params.hasAppExpansion()) {
      //If withAppStates, update params based on appIds of items files and do a appState index query.
      //After this filter the fileStates based on the results of the appState query
      for (FileOp fileOp : fileOps.getItems()) {
        Optional<String> appId = getAppId(fileOp);
        if(appId.isPresent()) {
          params.withAppExpansion(appId.get());
        }
      }
      Map<String, Map<Provenance.AppState, AppProvenanceHit>> appExps
        = elasticCtrl.provAppState(params.getAppStateFilter());
      Iterator<FileOp> fileOpIt = fileOps.getItems().iterator();
      while (fileOpIt.hasNext()) {
        FileOp fileOp = fileOpIt.next();
        Optional<String> appId = getAppId(fileOp);
        if(appId.isPresent() && appExps.containsKey(appId.get())) {
          Map<Provenance.AppState, AppProvenanceHit> appExp = appExps.get(appId.get());
          fileOp.setAppState(buildAppState(appExp));
        } else {
          fileOp.setAppState(MLAssetAppState.unknown());
        }
      }
    }
    return fileOps;
  }
  
  public FileOpDTO.Count provFileOpsCount(Project project, ProvFileOpsParamBuilder params)
    throws ServiceException, GenericException {
    return elasticCtrl.provFileOpsCount(project.getInode().getId(),
      params.getFileOpsFilterBy(), params.getFilterScripts(), params.getAggregations());
  }
  
  public FileOpDTO.Count provAppArtifactFootprint(Project project, ProvFileOpsParamBuilder params)
    throws GenericException, ServiceException {
    if(params.hasFileOpFilters()) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "footprint should have no predefined file operation filters");
    }
    params.withAggregation(ProvElasticHelper.ProvAggregations.ARTIFACT_FOOTPRINT);
    return elasticCtrl.provFileOpsCount(project.getInode().getId(), params.getFileOpsFilterBy(),
      params.getFilterScripts(), params.getAggregations());
  }
  public List<FootprintFileState> provAppFootprintList(Project project, ProvFileOpsParamBuilder params,
    AppFootprintType footprintType)
    throws GenericException, ServiceException {
    if(params.hasFileOpFilters()) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "footprint should have no predefined file operation filters");
    }
    if(!params.getAggregations().isEmpty()) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "aggregations currently only allowed with count");
    }
    addAppFootprintFileOps(params, footprintType);
    FileOpDTO.PList searchResult = provFileOpsList(project, params);
    List<FootprintFileState> appFootprint = processAppFootprintFileOps(searchResult.getItems(), footprintType);
    return appFootprint;
  }
  
  public Pair<Map<Long, FootprintFileStateTree>, Map<Long, FootprintFileStateTree>> provAppFootprintTree(
    Project project, ProvFileOpsParamBuilder params, AppFootprintType footprintType, boolean fullTree)
    throws GenericException, ServiceException {
    if(!params.getAggregations().isEmpty()) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "aggregations currently only allowed with count");
    }
    List<FootprintFileState> fileStates = provAppFootprintList(project, params, footprintType);
    Pair<Map<Long, ProvenanceController.BasicTreeBuilder<FootprintFileState>>,
      Map<Long, ProvenanceController.BasicTreeBuilder<FootprintFileState>>> result
      = processAsTree(project, fileStates, () -> new FootprintFileStateTree(), fullTree);
    return Pair.with((Map<Long, FootprintFileStateTree>)(Map)result.getValue0(),
      (Map<Long, FootprintFileStateTree>)(Map)(result.getValue1()));
  }
  
  public ProvFileOpsParamBuilder elasticTreeQueryParams(List<Long> inodeIds) throws GenericException {
    ProvFileOpsParamBuilder params = new ProvFileOpsParamBuilder()
      .filterByFileOperation(ProvFileOps.CREATE)
      .filterByFileOperation(ProvFileOps.DELETE);
    for(Long inodeId : inodeIds) {
      params.withFileInodeId(inodeId);
    }
    return params;
  }
  
  private FileOpDTO.PList cleanupFiles(Project project, Integer limit, Long beforeTimestamp)
    throws GenericException, ServiceException {
    ProvFileOpsParamBuilder params = new ProvFileOpsParamBuilder()
      .filterByField(ProvFileQuery.FileOps.PROJECT_I_ID, project.getInode().getId().toString())
      .filterByFileOperation(ProvFileOps.DELETE)
      .filterByField(ProvFileQuery.FileOpsAux.TIMESTAMP_LT, beforeTimestamp.toString())
      .sortByField(ProvElasticFields.FileOpsBase.TIMESTAMP, SortOrder.ASC)
      .withPagination(0, limit);
    return provFileOpsList(project, params);
  }
  
  private FileOpDTO.Count cleanupFilesSize(Project project, Integer limit, Long beforeTimestamp)
    throws GenericException, ServiceException {
    ProvFileOpsParamBuilder params = new ProvFileOpsParamBuilder()
      .filterByField(ProvFileQuery.FileOps.PROJECT_I_ID, project.getInode().getId().toString())
      .filterByFileOperation(ProvFileOps.DELETE)
      .filterByField(ProvFileQuery.FileOpsAux.TIMESTAMP_LT, beforeTimestamp.toString())
      .withPagination(0, limit);
    return provFileOpsCount(project, params);
  }
  
  public ArchiveDTO.Round cleanupRound(Project project, Integer limit) throws GenericException, ServiceException {
    Long beforeTimestamp = System.currentTimeMillis() - (settings.getProvArchiveDelay() * 1000);
    return cleanupRound(project, limit, beforeTimestamp);
  }
  
  public ArchiveDTO.Round cleanupRound(Project project, Integer limit, Long beforeTimestamp) throws GenericException,
    ServiceException {
    Long cleaned = 0l;
    for(FileOp fileOp : cleanupFiles(project, limit, beforeTimestamp).getItems()) {
      cleaned += elasticCtrl.provCleanupFilePrefix(project.getInode().getId(), fileOp.getInodeId(), Optional.empty());
      if(cleaned > limit) {
        break;
      }
    }
    return new ArchiveDTO.Round(0l, cleaned);
  }
  
  public ArchiveDTO.Round provCleanupFilePrefix(Project project, Long inodeId, Long timestamp)
    throws GenericException, ServiceException {
    Long cleaned =  elasticCtrl.provCleanupFilePrefix(project.getInode().getId(), inodeId, Optional.of(timestamp));
    return new ArchiveDTO.Round(0l, cleaned);
  }
  
  public ArchiveDTO.Round provCleanupFilePrefix(Project project, Long inodeId)
    throws GenericException, ServiceException {
    Long cleaned = elasticCtrl.provCleanupFilePrefix(project.getInode().getId(), inodeId, Optional.empty());
    return new ArchiveDTO.Round(0l, cleaned);
  }
  
  public ArchiveDTO.Round provCleanupFilePrefix(Project project, String docId, boolean skipDoc)
    throws ServiceException, GenericException {
    Long cleaned = elasticCtrl.provCleanupFilePrefix(project.getInode().getId(), docId, skipDoc);
    return new ArchiveDTO.Round(0l, cleaned);
  }
  
  public FileOpDTO.Count cleanupSize(Project project) throws GenericException, ServiceException {
    Long beforeTimestamp = System.currentTimeMillis() - ( settings.getProvArchiveDelay() * 1000);
    FileOpDTO.Count result = cleanupFilesSize(project, settings.getProvArchiveSize(), beforeTimestamp);
    return result;
  }
  
//  public ArchiveDTO.Round archiveRound() throws GenericException, ProjectException, ServiceException {
//    Long beforeTimestamp = System.currentTimeMillis() - ( settings.getProvArchiveDelay() * 1000);
//    return archiveRound(settings.getProvArchiveSize(), beforeTimestamp);
//  }
//
  public Pair<ArchiveDTO.Round, String> archiveRound(String nextToCheck, Integer limitIdx, Integer limitOps)
    throws GenericException, ServiceException {
    Long beforeTimestamp = System.currentTimeMillis() - ( settings.getProvArchiveDelay() * 1000);
    return archiveRound(nextToCheck, limitIdx, limitOps, beforeTimestamp);
  }
  
  public Pair<ArchiveDTO.Round, String> archiveRound(String nextToCheck, Integer limitIdx, Integer limitOps,
    Long beforeTimestamp)
    throws GenericException, ServiceException {
    String[] indices = elasticCtrl.getAllIndices();
    
    Long archived = 0l;
    Long cleaned = 0l;
    String nextToCheckAux = "";
    for(String indexName : indices) {
      if(cleaned > limitIdx) {
        nextToCheckAux = indexName;
        break;
      }
      if(indexName.compareTo(nextToCheck) < 0) {
        continue;
      }
      Project project = getProject(indexName);
      if(project == null) {
        LOGGER.log(Level.INFO, "deleting prov index:{0} with no corresponding project", indexName);
        elasticCtrl.deleteProvIndex(indexName);
        cleaned++;
        continue;
      }
      for (FileOp fileOp : cleanupFiles(project, limitOps, beforeTimestamp).getItems()) {
        archived += elasticCtrl.provArchiveFilePrefix(project.getInode().getId(), fileOp.getInodeId(), Optional.empty(),
          Utils.getProjectPath(fileOp.getProjectName()));
        if (archived > limitOps) {
          break;
        }
      }
    }
    return Pair.with(new ArchiveDTO.Round(archived, cleaned), nextToCheckAux);
  }
  
  private Project getProject(String indexName) throws GenericException, ServiceException {
    int endIndex = indexName.indexOf(Settings.PROV_FILE_INDEX_SUFFIX);
    String sInodeId = indexName.substring(0, endIndex);
    Long inodeId;
    try {
      inodeId = Long.parseLong(sInodeId);
    }catch(NumberFormatException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "error extracting project from prov index name - format error", e.getMessage(), e);
    }
    Inode inode = inodeFacade.findById(inodeId);
    if(inode == null) {
      return null;
    }
    Project project = projectFacade.findByInodeId(inode.getInodePK().getParentId(), inode.getInodePK().getName());
    return project;
  }
  
  public ArchiveDTO.Round projectArchiveRound(Project project, Integer limit)
    throws GenericException, ServiceException {
    Long beforeTimestamp = System.currentTimeMillis() - settings.getProvArchiveDelay();
    return projectArchiveRound(project, limit, beforeTimestamp);
  }
  public ArchiveDTO.Round projectArchiveRound(Project project, Integer limit, Long beforeTimestamp)
    throws GenericException, ServiceException {
    Long archived = 0l;
    for(FileOp fileOp : cleanupFiles(project, limit, beforeTimestamp).getItems()) {
      archived += elasticCtrl.provArchiveFilePrefix(project.getInode().getId(), fileOp.getInodeId(), Optional.empty(),
        Utils.getProjectPath(project.getName()));
    }
    return new ArchiveDTO.Round(archived, 0l);
  }
  
  private void addAppFootprintFileOps(ProvFileOpsParamBuilder params, AppFootprintType footprintType)
    throws GenericException {
    switch(footprintType) {
      case ALL:
        break;
      case INPUT:
        params
          .filterByFileOperation(ProvFileOps.CREATE)
          .filterByFileOperation(ProvFileOps.ACCESS_DATA);
        break;
      case OUTPUT:
        params
          .filterByFileOperation(ProvFileOps.CREATE)
          .filterByFileOperation(ProvFileOps.MODIFY_DATA)
          .filterByFileOperation(ProvFileOps.DELETE);
        break;
      case OUTPUT_ADDED:
      case TMP:
      case REMOVED:
        params
          .filterByFileOperation(ProvFileOps.CREATE)
          .filterByFileOperation(ProvFileOps.DELETE);
        break;
      default:
        throw new IllegalArgumentException("footprint filterType:" + footprintType + " not managed");
    }
  }
  
  private List<FootprintFileState> processAppFootprintFileOps(List<FileOp> fileOps,
    AppFootprintType footprintType) {
    Set<Long> inodeIds = new HashSet<>();
    List<FootprintFileState> files = new ArrayList<>();
    Set<Long> filesAccessed = new HashSet<>();
    Set<Long> filesCreated = new HashSet<>();
    Set<Long> filesModified = new HashSet<>();
    Set<Long> filesDeleted = new HashSet<>();
    for(FileOp fileOp : fileOps) {
      if (!inodeIds.contains(fileOp.getInodeId())) {
        FootprintFileState ffs = new FootprintFileState(fileOp.getInodeId(), fileOp.getInodeName(),
          fileOp.getParentInodeId(), fileOp.getProjectInodeId());
        inodeIds.add(ffs.getInodeId());
        files.add(ffs);
      }
      switch(fileOp.getInodeOperation()) {
        case "CREATE":
          filesCreated.add(fileOp.getInodeId());
          break;
        case "DELETE":
          filesDeleted.add(fileOp.getInodeId());
          break;
        case "ACCESS_DATA":
          filesAccessed.add(fileOp.getInodeId());
          break;
        case "MODIFY_DATA":
          filesModified.add(fileOp.getInodeId());
          break;
        default:
      }
    }
    //filter files based on footprintTypes
    switch(footprintType) {
      case ALL:
        //nothing - return all results
        break;
      case INPUT: {
        //files read - that existed before app (not created by app)
        Set<Long> aux = new HashSet<>(filesAccessed);
        aux.removeAll(filesCreated);
        files.removeIf((FootprintFileState fileState) -> !aux.contains(fileState.getInodeId()));
      } break;
      case OUTPUT: {
        //files created or modified, but not deleted
        Set<Long> aux = new HashSet<>(filesCreated);
        aux.addAll(filesModified);
        aux.removeAll(filesDeleted);
        files.removeIf((FootprintFileState fileState) -> !aux.contains(fileState.getInodeId()));
      } break;
      case OUTPUT_ADDED: {
        //files created but not deleted
        Set<Long> aux = new HashSet<>(filesCreated);
        aux.removeAll(filesDeleted);
        files.removeIf((FootprintFileState fileState) -> !aux.contains(fileState.getInodeId()));
      } break;
      case TMP: {
        //files created and deleted
        Set<Long> aux = new HashSet<>(filesCreated);
        aux.retainAll(filesDeleted);
        files.removeIf((FootprintFileState fileState) -> !aux.contains(fileState.getInodeId()));
      } break;
      case REMOVED: {
        //files not created and deleted
        Set<Long> aux = new HashSet<>(filesDeleted);
        aux.removeAll(filesCreated);
        files.removeIf((FootprintFileState fileState) -> !aux.contains(fileState.getInodeId()));
      } break;
      default:
        //continue;
    }
    return files;
  }
  
  private <S extends ProvenanceController.BasicFileState>
    Pair<Map<Long, ProvenanceController.BasicTreeBuilder<S>>, Map<Long, ProvenanceController.BasicTreeBuilder<S>>>
    processAsTree(Project project, List<S> fileStates, Supplier<BasicTreeBuilder<S>> instanceBuilder,
    boolean fullTree)
    throws GenericException, ServiceException {
    TreeHelper.TreeStruct<S> treeS = new TreeHelper.TreeStruct<>(instanceBuilder);
    treeS.processBasicFileState(fileStates);
    if(fullTree) {
      int maxDepth = 100;
      while(!treeS.complete() && maxDepth > 0 ) {
        maxDepth--;
        while (treeS.findInInodes()) {
          List<Long> inodeIdBatch = treeS.nextFindInInodes();
          List<Inode> inodeBatch = inodeFacade.findByIdList(inodeIdBatch);
          treeS.processInodeBatch(inodeIdBatch, inodeBatch);
        }
        while (treeS.findInProvenance()) {
          List<Long> inodeIdBatch = treeS.nextFindInProvenance();
          ProvFileOpsParamBuilder elasticPathQueryParams = elasticTreeQueryParams(inodeIdBatch);
          FileOpDTO.PList inodeBatch = provFileOpsList(project, elasticPathQueryParams);
          treeS.processProvenanceBatch(inodeIdBatch, inodeBatch.getItems());
        }
      }
      return treeS.getFullTree();
    } else {
      return treeS.getMinTree();
    }
  }
  
  private MLAssetAppState buildAppState(Map<Provenance.AppState, AppProvenanceHit> appStates)
    throws ServiceException {
    MLAssetAppState mlAssetAppState = new MLAssetAppState();
    //app states is an ordered map
    //I assume values will still be ordered based on keys
    //if this is the case, the correct progression is SUBMITTED->RUNNING->FINISHED/KILLED/FAILED
    //as such just iterating over the states will provide us with the correct current state
    for (AppProvenanceHit appState : appStates.values()) {
      mlAssetAppState.setAppState(appState.getAppState(), appState.getAppStateTimestamp());
    }
    return mlAssetAppState;
  }
  
  //throw new IllegalArgumentException("withAppId enabled for tls clusters or notls cluster with xattr appIds");
  private Optional<String> getAppId(FileState fileState) {
    if(fileState.getAppId().equals("notls") // TODO remove once it uses new hops
      || fileState.getAppId().equals("none")) {
      if(fileState.getXattrs().containsKey("appId")) {
        return Optional.of(fileState.getXattrs().get("appId"));
      } else {
        return Optional.empty();
      }
    } else {
      return Optional.of(fileState.getAppId());
    }
  }
  
  private Optional<String> getAppId(FileOp fileOp) {
    if(fileOp.getAppId().equals("notls")) {
      return Optional.empty();
    } else {
      return Optional.of(fileOp.getAppId());
    }
  }
  
  public interface BasicFileState {
    Long getInodeId();
    String getInodeName();
    Long getProjectInodeId();
    boolean isProject();
    Long getParentInodeId();
  }
  
  public interface BasicTreeBuilder<S extends BasicFileState> {
    void setInodeId(Long inodeId);
    Long getInodeId();
    void setName(String name);
    String getName();
    void setFileState(S fileState);
    S getFileState();
    void addChild(BasicTreeBuilder<S> child) throws GenericException;
  }
  
  public ArchiveDTO.Base getArchiveDoc(Project project, Long inodeId) throws ServiceException, GenericException {
    return elasticCtrl.getArchive(project.getInode().getId(), inodeId);
  }
}
