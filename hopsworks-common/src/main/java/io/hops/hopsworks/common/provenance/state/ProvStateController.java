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
package io.hops.hopsworks.common.provenance.state;

import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.provenance.elastic.prov.ProvElasticController;
import io.hops.hopsworks.common.provenance.ops.ProvAppHelper;
import io.hops.hopsworks.common.provenance.state.dto.ProvAppStateElastic;
import io.hops.hopsworks.common.provenance.state.dto.ProvFileStateElastic;
import io.hops.hopsworks.common.provenance.state.dto.ProvFileStateTreeDTO;
import io.hops.hopsworks.common.provenance.state.dto.ProvMLAssetAppStateDTO;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.state.dto.ProvFileStateDTO;
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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProvStateController {
  private final static Logger LOGGER = Logger.getLogger(ProvStateController.class.getName());
  
  @EJB
  private ProvElasticController elasticCtrl;
  @EJB
  private ProvTreeController treeCtrl;
  
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
          fileState.setAppState(ProvAppHelper.buildAppState(appExp));
        } else {
          fileState.setAppState(ProvMLAssetAppStateDTO.unknown());
        }
      }
    }
    return fileStates;
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
  
  public Pair<Map<Long, ProvFileStateTreeDTO>, Map<Long, ProvFileStateTreeDTO>> provFileStateTree(
    Project project, ProvFileStateParamBuilder params, boolean fullTree)
    throws ProvenanceException, ServiceException {
    List<ProvFileStateElastic> fileStates = provFileStateList(project, params).getItems();
    Pair<Map<Long, ProvTree.Builder<ProvFileStateElastic>>,
      Map<Long, ProvTree.Builder<ProvFileStateElastic>>> result
      = treeCtrl.processAsTree(fileStates, () -> new ProvFileStateTreeDTO(), fullTree);
    return Pair.with((Map<Long, ProvFileStateTreeDTO>)(Map)result.getValue0(),
      (Map<Long, ProvFileStateTreeDTO>)(Map)(result.getValue1()));
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
}
