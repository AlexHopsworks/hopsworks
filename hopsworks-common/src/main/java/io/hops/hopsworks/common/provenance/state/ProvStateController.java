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
package io.hops.hopsworks.common.provenance.state;

import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.provenance.app.apiToElastic.ProvAppController;
import io.hops.hopsworks.common.provenance.core.apiToElastic.ProvParser;
import io.hops.hopsworks.common.provenance.core.elastic.BasicElasticHit;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticCache;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticClient;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticHelper;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticHitsHandler;
import io.hops.hopsworks.common.provenance.util.ProvElasticHelper;
import io.hops.hopsworks.common.provenance.ops.ProvAppHelper;
import io.hops.hopsworks.common.provenance.state.dto.ProvAppStateElastic;
import io.hops.hopsworks.common.provenance.state.dto.ProvFileStateElastic;
import io.hops.hopsworks.common.provenance.state.dto.ProvFileStateTreeDTO;
import io.hops.hopsworks.common.provenance.state.dto.ProvMLAssetAppStateDTO;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.state.dto.ProvFileStateDTO;
import io.hops.hopsworks.common.provenance.util.functional.CheckedFunction;
import io.hops.hopsworks.common.provenance.util.functional.CheckedSupplier;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProvStateController {
  private final static Logger LOGGER = Logger.getLogger(ProvStateController.class.getName());
  
  @EJB
  private Settings settings;
  @EJB
  private ElasticClient client;
  @EJB
  private ProvTreeController treeCtrl;
  @EJB
  private ProvAppController appCtrl;
  @EJB
  private ElasticCache cache;
  
  public ProvFileStateDTO.PList provFileStateList(Project project, ProvFileStateParamBuilder params)
    throws ProvenanceException, ServiceException {
    if(params.getPagination() != null && !params.getAppStateFilter().isEmpty()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
        "cannot use pagination with app state filtering");
    }
    
    checkMapping(project, params);
    ProvFileStateDTO.PList fileStates = provFileState(project.getInode().getId(),
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
        = appCtrl.provAppState(params.getAppStateFilter());
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
    return provFileStateCount(project.getInode().getId(), params.getFileStateFilter(),
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
    Map<String, String> mapping = cache.mngIndexGetMapping(index, false);
    if(mapping == null) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.BAD_REQUEST, Level.INFO,
        "provenance file state - no index");
    }
    try {
      params.fixSortBy(index, mapping);
    } catch(ServiceException e) {
      mapping = cache.mngIndexGetMapping(index, true);
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
  
  private ProvFileStateDTO.PList provFileState(Long projectIId,
    Map<String, ProvParser.FilterVal> fileStateFilters,
    List<Pair<ProvParser.Field, SortOrder>> fileStateSortBy,
    Map<String, String> xAttrsFilters, Map<String, String> likeXAttrsFilters, Set<String> hasXAttrsFilters,
    List<ProvFileStateParamBuilder.SortE> xattrSortBy,
    Integer offset, Integer limit)
    throws ProvenanceException, ServiceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.baseSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE,
        settings.getElasticDefaultScrollPageSize())
        .andThen(filterByStateParams(fileStateFilters, xAttrsFilters, likeXAttrsFilters, hasXAttrsFilters))
        .andThen(ElasticHelper.withFileStateOrder(fileStateSortBy, xattrSortBy))
        .andThen(ElasticHelper.withPagination(offset, limit, settings.getElasticMaxScrollPageSize()));
    SearchRequest request = srF.get();
    Pair<Long, List<ProvFileStateElastic>> searchResult = client.search(request, fileStateParser());
    return new ProvFileStateDTO.PList(searchResult.getValue1(), searchResult.getValue0());
  }
  
  private Long provFileStateCount(Long projectIId,
    Map<String, ProvParser.FilterVal> fileStateFilters,
    Map<String, String> xAttrsFilters, Map<String, String> likeXAttrsFilters, Set<String> hasXAttrsFilters)
    throws ProvenanceException, ServiceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.countSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE)
        .andThen(filterByStateParams(fileStateFilters, xAttrsFilters, likeXAttrsFilters, hasXAttrsFilters));
    SearchRequest request = srF.get();
    Long searchResult = client.searchCount(request);
    return searchResult;
  }
  
  private ElasticHitsHandler<ProvFileStateElastic, List<ProvFileStateElastic>, ?, ProvenanceException>
    fileStateParser() {
    return ElasticHitsHandler.instanceAddToList(
      (BasicElasticHit hit) -> ProvFileStateElastic.instance(hit, settings.getHopsRpcTls()));
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> filterByStateParams(
    Map<String, ProvParser.FilterVal> fileStateFilters,
    Map<String, String> xAttrsFilters, Map<String, String> likeXAttrsFilters, Set<String> hasXAttrsFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvParser.BaseField.ENTRY_TYPE.toString().toLowerCase(),
          ProvParser.EntryType.STATE.toString().toLowerCase()));
      query = ProvElasticHelper.filterByBasicFields(query, fileStateFilters);
      for (Map.Entry<String, String> filter : xAttrsFilters.entrySet()) {
        query = query.must(getXAttrQB(filter.getKey(), filter.getValue()));
      }
      for (Map.Entry<String, String> filter : likeXAttrsFilters.entrySet()) {
        query = query.must(getLikeXAttrQB(filter.getKey(), filter.getValue()));
      }
      for(String xattrKey : hasXAttrsFilters) {
        query = query.must(hasXAttrQB(xattrKey));
      }
      sr.source().query(query);
      return sr;
    };
  }
  
  public QueryBuilder hasXAttrQB(String xattrAdjustedKey) {
    return existsQuery(xattrAdjustedKey);
  }
  
  public QueryBuilder getXAttrQB(String xattrAdjustedKey, String xattrVal) {
    return termQuery(xattrAdjustedKey, xattrVal.toLowerCase());
  }
  
  public QueryBuilder getLikeXAttrQB(String xattrAdjustedKey, String xattrVal) {
    return ElasticHelper.fullTextSearch(xattrAdjustedKey, xattrVal);
  }
}
