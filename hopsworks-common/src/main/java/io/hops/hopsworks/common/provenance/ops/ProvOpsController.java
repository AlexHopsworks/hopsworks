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
package io.hops.hopsworks.common.provenance.ops;

import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.provenance.core.ProvExecution;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.elastic.prov.ProvElasticController;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpDTO;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpElastic;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFootprintFileStateTreeElastic;
import io.hops.hopsworks.common.provenance.state.ProvTree;
import io.hops.hopsworks.common.provenance.state.ProvTreeController;
import io.hops.hopsworks.common.provenance.state.dto.ProvFileStateMinDTO;
import io.hops.hopsworks.common.provenance.state.dto.ProvAppStateElastic;
import io.hops.hopsworks.common.provenance.state.dto.ProvMLAssetAppStateDTO;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
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
import java.util.logging.Level;

@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProvOpsController {
  
  @EJB
  private ProvElasticController elasticCtrl;
  @EJB
  private ProvTreeController treeCtrl;
  
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
          fileOp.setAppState(ProvAppHelper.buildAppState(appExp));
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
  
  public Pair<Map<Long, ProvFootprintFileStateTreeElastic>, Map<Long, ProvFootprintFileStateTreeElastic>>
    provAppFootprintTree(Project project, ProvFileOpsParamBuilder params, Provenance.FootprintType footprintType,
    boolean fullTree)
    throws ProvenanceException, ServiceException {
    if(!params.getAggregations().isEmpty()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
        "aggregations currently only allowed with count");
    }
    List<ProvFileStateMinDTO> fileStates = provAppFootprintList(project, params, footprintType);
    Pair<Map<Long, ProvTree.Builder<ProvFileStateMinDTO>>,
      Map<Long, ProvTree.Builder<ProvFileStateMinDTO>>> result
      = treeCtrl.processAsTree(fileStates, () -> new ProvFootprintFileStateTreeElastic(), fullTree);
    return Pair.with((Map<Long, ProvFootprintFileStateTreeElastic>)(Map)result.getValue0(),
      (Map<Long, ProvFootprintFileStateTreeElastic>)(Map)(result.getValue1()));
  }
  
  private Optional<String> getAppId(ProvFileOpElastic fileOp) {
    if(fileOp.getAppId().equals("none")) {
      return Optional.empty();
    } else {
      return Optional.of(fileOp.getAppId());
    }
  }
}
