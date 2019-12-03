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
package io.hops.hopsworks.common.provenance.elastic.prov;

import io.hops.hopsworks.common.dao.hdfs.inode.Inode;
import io.hops.hopsworks.common.dao.hdfs.inode.InodeFacade;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.hdfs.Utils;
import io.hops.hopsworks.common.provenance.core.ProvExecution;
import io.hops.hopsworks.common.provenance.xml.ProvFileStateMinDTO;
import io.hops.hopsworks.common.provenance.xml.ProvMLAssetAppStateDTO;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvElasticFields;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvFileOpsParamBuilder;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvFileQuery;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.xml.ProvArchiveDTO;
import io.hops.hopsworks.common.provenance.xml.ProvFileOpDTO;
import io.hops.hopsworks.common.provenance.xml.ProvFileStateDTO;
import io.hops.hopsworks.common.provenance.xml.ProvFileStateTreeDTO;
import io.hops.hopsworks.common.provenance.xml.ProvMLStateMinDTO;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.util.ArrayList;
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
  
  public ProvFileStateDTO.PList provFileStateList(Project project, ProvFileStateParamBuilder params)
    throws ProvenanceException, ServiceException {
    if(params.getPagination() != null && !params.getAppStateFilter().isEmpty()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
        "cannot use pagination with app state filtering");
    }
  
    checkMapping(project, params);
    ProvFileStateDTO.PList fileStates = elasticCtrl.provFileState(project.getInode().getId(),
      params.getFileStateFilter(), params.getFileStateSortBy(),
      params.getExactXAttrFilter(), params.getLikeXAttrFilter(), params.getHasXAttrFilter(),
      params.getXAttrSortBy(), params.getPagination().getValue0(), params.getPagination().getValue1());

    if (params.hasAppExpansion()) {
      //If withAppStates, update params based on appIds of items files and do a appState index query.
      //After this filter the fileStates based on the results of the appState query
      for (ProvFileStateElastic fileState : fileStates.getItems()) {
        Optional<String> appId = getAppId(fileState);
        if(appId.isPresent()) {
          params.withAppExpansion(appId.get());
        }
      }
      Map<String, Map<Provenance.AppState, ProvAppStateElastic>> appExps
        = elasticCtrl.provAppState(params.getAppStateFilter());
      Iterator<ProvFileStateElastic> fileStateIt = fileStates.getItems().iterator();
      while(fileStateIt.hasNext()) {
        ProvFileStateElastic fileState = fileStateIt.next();
        Optional<String> appId = getAppId(fileState);
        if(appId.isPresent() && appExps.containsKey(appId.get())) {
          Map<Provenance.AppState, ProvAppStateElastic> appExp = appExps.get(appId.get());
          fileState.setAppState(buildAppState(appExp));
        } else {
          fileState.setAppState(ProvMLAssetAppStateDTO.unknown());
        }
      }
    }
    return fileStates;
  }
  
  private void checkMapping(Project project, ProvFileStateParamBuilder params)
    throws ProvenanceException, ServiceException {
    String index = Provenance.getProjectIndex(project);
    Map<String, String> mapping = elasticCtrl.mngIndexGetMapping(index, false);
    if(mapping == null) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.BAD_REQUEST, Level.INFO,
        "provenance file state - no index");
    }
    try {
      params.fixSortBy(index, mapping);
    } catch(ServiceException e) {
      mapping = elasticCtrl.mngIndexGetMapping(index, true);
      params.fixSortBy(index, mapping);
    }
  }
  
  public Pair<Map<Long, ProvFileStateTreeDTO>, Map<Long, ProvFileStateTreeDTO>> provFileStateTree(Project project,
    ProvFileStateParamBuilder params, boolean fullTree)
    throws ProvenanceException, ServiceException {
    List<ProvFileStateElastic> fileStates = provFileStateList(project, params).getItems();
    Pair<Map<Long, ProvenanceController.BasicTreeBuilder<ProvFileStateElastic>>,
      Map<Long, ProvenanceController.BasicTreeBuilder<ProvFileStateElastic>>> result
      = processAsTree(project, fileStates, () -> new ProvFileStateTreeDTO(), fullTree);
    return Pair.with((Map<Long, ProvFileStateTreeDTO>)(Map)result.getValue0(),
      (Map<Long, ProvFileStateTreeDTO>)(Map)(result.getValue1()));
  }
  
  public long provFileStateCount(Project project, ProvFileStateParamBuilder params)
    throws ProvenanceException, ServiceException {
    if(params.hasAppExpansion()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
        "provenance file state count does not currently work with app state expansion");
    }
    return elasticCtrl.provFileStateCount(project.getInode().getId(), params.getFileStateFilter(),
      params.getExactXAttrFilter(), params.getLikeXAttrFilter(), params.getHasXAttrFilter());
  }
  
  public ProvFileOpDTO.PList provFileOpsList(Project project, ProvFileOpsParamBuilder params)
    throws ProvenanceException, ServiceException {
    if(!params.getAggregations().isEmpty()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
        "aggregations currently only allowed with count");
    }
    ProvFileOpDTO.PList fileOps;
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
      for (ProvFileOpElastic fileOp : fileOps.getItems()) {
        Optional<String> appId = getAppId(fileOp);
        if(appId.isPresent()) {
          params.withAppExpansion(appId.get());
        }
      }
      Map<String, Map<Provenance.AppState, ProvAppStateElastic>> appExps
        = elasticCtrl.provAppState(params.getAppStateFilter());
      Iterator<ProvFileOpElastic> fileOpIt = fileOps.getItems().iterator();
      while (fileOpIt.hasNext()) {
        ProvFileOpElastic fileOp = fileOpIt.next();
        Optional<String> appId = getAppId(fileOp);
        if(appId.isPresent() && appExps.containsKey(appId.get())) {
          Map<Provenance.AppState, ProvAppStateElastic> appExp = appExps.get(appId.get());
          fileOp.setAppState(buildAppState(appExp));
        } else {
          fileOp.setAppState(ProvMLAssetAppStateDTO.unknown());
        }
      }
    }
    return fileOps;
  }
  
  public ProvFileOpDTO.Count provFileOpsCount(Project project, ProvFileOpsParamBuilder params)
    throws ServiceException, ProvenanceException {
    return elasticCtrl.provFileOpsCount(project.getInode().getId(),
      params.getFileOpsFilterBy(), params.getFilterScripts(), params.getAggregations());
  }
  
  public ProvFileOpDTO.Count provAppArtifactFootprint(Project project, ProvFileOpsParamBuilder params)
    throws ProvenanceException, ServiceException {
    if(params.hasFileOpFilters()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.BAD_REQUEST, Level.INFO,
        "footprint should have no predefined file operation filters");
    }
    params.withAggregation(ProvElasticAggregations.ProvAggregations.ARTIFACT_FOOTPRINT);
    return elasticCtrl.provFileOpsCount(project.getInode().getId(), params.getFileOpsFilterBy(),
      params.getFilterScripts(), params.getAggregations());
  }
  public List<ProvFileStateMinDTO> provAppFootprintList(Project project, ProvFileOpsParamBuilder params,
    Provenance.FootprintType footprintType)
    throws ProvenanceException, ServiceException {
    if(params.hasFileOpFilters()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.BAD_REQUEST, Level.INFO,
        "footprint should have no predefined file operation filters");
    }
    if(!params.getAggregations().isEmpty()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
        "aggregations currently only allowed with count");
    }
    ProvExecution.addFootprintQueryParams(params, footprintType);
    ProvFileOpDTO.PList searchResult = provFileOpsList(project, params);
    Map<Long, ProvFileStateMinDTO> footprint = ProvExecution.processFootprint(searchResult.getItems(), footprintType);
    return new ArrayList<>(footprint.values());
  }
  
  public Pair<Map<Long, ProvFootprintFileStateTreeElastic>, Map<Long, ProvFootprintFileStateTreeElastic>>
    provAppFootprintTree(Project project, ProvFileOpsParamBuilder params, Provenance.FootprintType footprintType,
    boolean fullTree)
    throws ProvenanceException, ServiceException {
    if(!params.getAggregations().isEmpty()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
        "aggregations currently only allowed with count");
    }
    List<ProvFileStateMinDTO> fileStates = provAppFootprintList(project, params, footprintType);
    Pair<Map<Long, ProvenanceController.BasicTreeBuilder<ProvFileStateMinDTO>>,
      Map<Long, ProvenanceController.BasicTreeBuilder<ProvFileStateMinDTO>>> result
      = processAsTree(project, fileStates, () -> new ProvFootprintFileStateTreeElastic(), fullTree);
    return Pair.with((Map<Long, ProvFootprintFileStateTreeElastic>)(Map)result.getValue0(),
      (Map<Long, ProvFootprintFileStateTreeElastic>)(Map)(result.getValue1()));
  }
  
  public ProvFileOpsParamBuilder elasticTreeQueryParams(List<Long> inodeIds) throws ProvenanceException {
    ProvFileOpsParamBuilder params = new ProvFileOpsParamBuilder()
      .filterByFileOperation(Provenance.FileOps.CREATE)
      .filterByFileOperation(Provenance.FileOps.DELETE);
    for(Long inodeId : inodeIds) {
      params.withFileInodeId(inodeId);
    }
    return params;
  }
  
  private ProvFileOpDTO.PList cleanupFiles(Project project, Integer limit, Long beforeTimestamp)
    throws ProvenanceException, ServiceException {
    ProvFileOpsParamBuilder params = new ProvFileOpsParamBuilder()
      .filterByField(ProvFileQuery.FileOps.PROJECT_I_ID, project.getInode().getId().toString())
      .filterByFileOperation(Provenance.FileOps.DELETE)
      .filterByField(ProvFileQuery.FileOpsAux.TIMESTAMP_LT, beforeTimestamp.toString())
      .sortByField(ProvElasticFields.FileOpsBase.TIMESTAMP, SortOrder.ASC)
      .withPagination(0, limit);
    return provFileOpsList(project, params);
  }
  
  private ProvFileOpDTO.Count cleanupFilesSize(Project project, Integer limit, Long beforeTimestamp)
    throws ProvenanceException, ServiceException {
    ProvFileOpsParamBuilder params = new ProvFileOpsParamBuilder()
      .filterByField(ProvFileQuery.FileOps.PROJECT_I_ID, project.getInode().getId().toString())
      .filterByFileOperation(Provenance.FileOps.DELETE)
      .filterByField(ProvFileQuery.FileOpsAux.TIMESTAMP_LT, beforeTimestamp.toString())
      .withPagination(0, limit);
    return provFileOpsCount(project, params);
  }
  
  public ProvArchiveDTO.Round cleanupRound(Project project, Integer limit)
    throws ProvenanceException, ServiceException {
    Long beforeTimestamp = System.currentTimeMillis() - (settings.getProvArchiveDelay() * 1000);
    return cleanupRound(project, limit, beforeTimestamp);
  }
  
  public ProvArchiveDTO.Round cleanupRound(Project project, Integer limit, Long beforeTimestamp)
    throws ProvenanceException, ServiceException {
    Long cleaned = 0l;
    for(ProvFileOpElastic fileOp : cleanupFiles(project, limit, beforeTimestamp).getItems()) {
      cleaned += elasticCtrl.provCleanupFilePrefix(project.getInode().getId(), fileOp.getInodeId(), Optional.empty());
      if(cleaned > limit) {
        break;
      }
    }
    return new ProvArchiveDTO.Round(0l, cleaned);
  }
  
  public ProvArchiveDTO.Round provCleanupFilePrefix(Project project, Long inodeId, Long timestamp)
    throws ProvenanceException, ServiceException {
    Long cleaned =  elasticCtrl.provCleanupFilePrefix(project.getInode().getId(), inodeId, Optional.of(timestamp));
    return new ProvArchiveDTO.Round(0l, cleaned);
  }
  
  public ProvArchiveDTO.Round provCleanupFilePrefix(Project project, Long inodeId)
    throws ProvenanceException, ServiceException {
    Long cleaned = elasticCtrl.provCleanupFilePrefix(project.getInode().getId(), inodeId, Optional.empty());
    return new ProvArchiveDTO.Round(0l, cleaned);
  }
  
  public ProvArchiveDTO.Round provCleanupFilePrefix(Project project, String docId, boolean skipDoc)
    throws ServiceException, ProvenanceException {
    Long cleaned = elasticCtrl.provCleanupFilePrefix(project.getInode().getId(), docId, skipDoc);
    return new ProvArchiveDTO.Round(0l, cleaned);
  }
  
  public ProvFileOpDTO.Count cleanupSize(Project project) throws ProvenanceException, ServiceException {
    Long beforeTimestamp = System.currentTimeMillis() - ( settings.getProvArchiveDelay() * 1000);
    ProvFileOpDTO.Count result = cleanupFilesSize(project, settings.getProvArchiveSize(), beforeTimestamp);
    return result;
  }
  
  public Pair<ProvArchiveDTO.Round, String> archiveRound(String nextToCheck, Integer limitIdx, Integer limitOps)
    throws ProvenanceException, ServiceException {
    Long beforeTimestamp = System.currentTimeMillis() - ( settings.getProvArchiveDelay() * 1000);
    return archiveRound(nextToCheck, limitIdx, limitOps, beforeTimestamp);
  }
  
  public Pair<ProvArchiveDTO.Round, String> archiveRound(String nextToCheck, Integer limitIdx, Integer limitOps,
    Long beforeTimestamp)
    throws ProvenanceException, ServiceException {
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
      for (ProvFileOpElastic fileOp : cleanupFiles(project, limitOps, beforeTimestamp).getItems()) {
        archived += elasticCtrl.provArchiveFilePrefix(project.getInode().getId(), fileOp.getInodeId(), Optional.empty(),
          Utils.getProjectPath(fileOp.getProjectName()));
        if (archived > limitOps) {
          break;
        }
      }
    }
    return Pair.with(new ProvArchiveDTO.Round(archived, cleaned), nextToCheckAux);
  }
  
  private Project getProject(String indexName) throws ProvenanceException {
    int endIndex = indexName.indexOf(Settings.PROV_FILE_INDEX_SUFFIX);
    String sInodeId = indexName.substring(0, endIndex);
    Long inodeId;
    try {
      inodeId = Long.parseLong(sInodeId);
    }catch(NumberFormatException e) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.INTERNAL_ERROR, Level.WARNING,
        "error extracting project from prov index name - format error", e.getMessage(), e);
    }
    Inode inode = inodeFacade.findById(inodeId);
    if(inode == null) {
      return null;
    }
    Project project = projectFacade.findByInodeId(inode.getInodePK().getParentId(), inode.getInodePK().getName());
    return project;
  }
  
  public ProvArchiveDTO.Round projectArchiveRound(Project project, Integer limit)
    throws ProvenanceException, ServiceException {
    Long beforeTimestamp = System.currentTimeMillis() - settings.getProvArchiveDelay();
    return projectArchiveRound(project, limit, beforeTimestamp);
  }
  public ProvArchiveDTO.Round projectArchiveRound(Project project, Integer limit, Long beforeTimestamp)
    throws ProvenanceException, ServiceException {
    Long archived = 0L;
    for(ProvFileOpElastic fileOp : cleanupFiles(project, limit, beforeTimestamp).getItems()) {
      archived += elasticCtrl.provArchiveFilePrefix(project.getInode().getId(), fileOp.getInodeId(), Optional.empty(),
        Utils.getProjectPath(project.getName()));
    }
    return new ProvArchiveDTO.Round(archived, 0l);
  }
  
  private <S extends ProvenanceController.BasicFileState>
    Pair<Map<Long, ProvenanceController.BasicTreeBuilder<S>>, Map<Long, ProvenanceController.BasicTreeBuilder<S>>>
    processAsTree(Project project, List<S> fileStates, Supplier<BasicTreeBuilder<S>> instanceBuilder,
    boolean fullTree)
    throws ProvenanceException, ServiceException {
    ProvTreeBuilderHelper.TreeStruct<S> treeS = new ProvTreeBuilderHelper.TreeStruct<>(instanceBuilder);
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
          ProvFileOpDTO.PList inodeBatch = provFileOpsList(project, elasticPathQueryParams);
          treeS.processProvenanceBatch(inodeIdBatch, inodeBatch.getItems());
        }
      }
      return treeS.getFullTree();
    } else {
      return treeS.getMinTree();
    }
  }
  
  private ProvMLAssetAppStateDTO buildAppState(Map<Provenance.AppState, ProvAppStateElastic> appStates)
    throws ServiceException {
    ProvMLAssetAppStateDTO mlAssetAppState = new ProvMLAssetAppStateDTO();
    //app states is an ordered map
    //I assume values will still be ordered based on keys
    //if this is the case, the correct progression is SUBMITTED->RUNNING->FINISHED/KILLED/FAILED
    //as such just iterating over the states will provide us with the correct current state
    for (ProvAppStateElastic appState : appStates.values()) {
      mlAssetAppState.setAppState(appState.getAppState(), appState.getAppStateTimestamp());
    }
    return mlAssetAppState;
  }
  
  private Optional<String> getAppId(ProvFileStateElastic fileState) {
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
  
  private Optional<String> getAppId(ProvFileOpElastic fileOp) {
    if(fileOp.getAppId().equals("notls") // TODO remove once it uses new hops
      || fileOp.getAppId().equals("none")) {
      return Optional.empty();
    } else {
      return Optional.of(fileOp.getAppId());
    }
  }
  
  //TODO Alex - maybe cleanup
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
    void addChild(BasicTreeBuilder<S> child) throws ProvenanceException;
  }
  
  public ProvArchiveDTO.Base getArchiveDoc(Project project, Long inodeId) throws ServiceException, ProvenanceException {
    return elasticCtrl.getArchive(project.getInode().getId(), inodeId);
  }
  
  //ML footprint
  
  
  public Map<String, ProvMLStateMinDTO> provExperimentFootprintML(Project project, String experimentId,
    Provenance.FootprintType footprintType) throws ProvenanceException, ServiceException {
    //setup query params
    ProvFileOpsParamBuilder opsParams = new ProvFileOpsParamBuilder()
      .filterByField(ProvFileQuery.FileOps.ML_TYPE, Provenance.MLType.EXPERIMENT.toString())
      .filterByField(ProvFileQuery.FileOps.ML_ID, experimentId);
    ProvExecution.addFootprintQueryParams(opsParams, footprintType);
    //execute query
    ProvExecution.Footprint<String, ProvMLStateMinDTO> rawMLFootprint = elasticCtrl.provFileOpsScrolling(
      project.getInode().getId(),
      opsParams.getFileOpsFilterBy(),
      opsParams.getFilterScripts(),
      ProvExecution.mlStateProc());
    //parse result
    Map<String, ProvMLStateMinDTO> footprint = ProvExecution.processFootprint(rawMLFootprint, footprintType);
    return footprint;
  }
  
  //Testing
  public Map<String, String> mngIndexGetMapping(String indexRegex, boolean forceFetch) throws ServiceException {
    return elasticCtrl.mngIndexGetMapping(indexRegex, forceFetch);
  }
  
  public String experimentCreator(Project project, String mlId) throws ProvenanceException {
    return ProvExecution.experimentCreator(elasticCtrl, project, mlId);
  }
  
  public String trainigDatasetCreator(Project project, String mlId) throws ProvenanceException {
    return ProvExecution.trainigDatasetCreator(elasticCtrl, project, mlId);
  }
  
  public Set<String> trainigDatasetUser(Project project, String mlId) throws ProvenanceException {
    return ProvExecution.trainigDatasetUser(elasticCtrl, project, mlId);
  }
}
