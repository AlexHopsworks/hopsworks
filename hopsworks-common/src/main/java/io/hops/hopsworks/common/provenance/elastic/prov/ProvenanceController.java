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

import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.hdfs.inode.Inode;
import io.hops.hopsworks.common.dao.hdfs.inode.InodeFacade;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvFileOpsParamBuilder;
import io.hops.hopsworks.common.provenance.core.ProvExecution;
import io.hops.hopsworks.common.provenance.xml.ProvFileOpDTO;
import io.hops.hopsworks.common.provenance.xml.ProvFileStateMinDTO;
import io.hops.hopsworks.common.provenance.xml.ProvFileStateTreeDTO;
import io.hops.hopsworks.common.provenance.xml.ProvMLAssetAppStateDTO;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.xml.ProvFileStateDTO;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.ArrayList;
import java.util.List;
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
  
  public long provFileStateCount(Project project, ProvFileStateParamBuilder params)
    throws ProvenanceException, ServiceException {
    if(params.hasAppExpansion()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
        "provenance file state count does not currently work with app state expansion");
    }
    return elasticCtrl.provFileStateCount(project.getInode().getId(), params.getFileStateFilter(),
      params.getExactXAttrFilter(), params.getLikeXAttrFilter(), params.getHasXAttrFilter());
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
    if(fileState.getAppId().equals("none")) {
      if(fileState.getXattrs().containsKey("appId")) {
        return Optional.of(fileState.getXattrs().get("appId"));
      } else {
        return Optional.empty();
      }
    } else {
      return Optional.of(fileState.getAppId());
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
  
  public ProvFileOpsParamBuilder elasticTreeQueryParams(List<Long> inodeIds) throws ProvenanceException {
    ProvFileOpsParamBuilder params = new ProvFileOpsParamBuilder()
      .filterByFileOperation(Provenance.FileOps.CREATE)
      .filterByFileOperation(Provenance.FileOps.DELETE);
    for(Long inodeId : inodeIds) {
      params.withFileInodeId(inodeId);
    }
    return params;
  }
  
  private Optional<String> getAppId(ProvFileOpElastic fileOp) {
    if(fileOp.getAppId().equals("none")) {
      return Optional.empty();
    } else {
      return Optional.of(fileOp.getAppId());
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
}
