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
import io.hops.hopsworks.common.provenance.app.ProvAppController;
import io.hops.hopsworks.common.provenance.app.ProvAppHelper;
import io.hops.hopsworks.common.provenance.app.dto.ProvAppStateDTO;
import io.hops.hopsworks.common.provenance.app.dto.ProvAppStateElastic;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.core.ProvParser;
import io.hops.hopsworks.common.provenance.core.elastic.BasicElasticHit;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticAggregationParser;
import io.hops.hopsworks.common.provenance.core.elastic.ProvElasticController;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticHelper;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticHitsHandler;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpDTO;
import io.hops.hopsworks.common.provenance.ops.dto.ProvOpsElastic;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFootprintFileStateTreeElastic;
import io.hops.hopsworks.common.provenance.state.ProvTree;
import io.hops.hopsworks.common.provenance.state.ProvTreeController;
import io.hops.hopsworks.common.provenance.ops.dto.ProvStateMinDTO;
import io.hops.hopsworks.common.provenance.util.ProvHelper;
import io.hops.hopsworks.common.provenance.util.functional.CheckedFunction;
import io.hops.hopsworks.common.provenance.util.functional.CheckedSupplier;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ElasticException;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProvOpsController {
  
  @EJB
  private ProvElasticController client;
  @EJB
  private ProvAppController appCtrl;
  @EJB
  private ProvTreeController treeCtrl;
  @EJB
  private Settings settings;
  
  
  public ProvFileOpDTO.Count provFileOpsCount(Project project, ProvFileOpsParamBuilder params)
    throws ProvenanceException {
    return provFileOpsCount(project.getInode().getId(),
      params.getFileOpsFilterBy(), params.getAggregations());
  }
  
  public ProvFileOpDTO.Count provAppArtifactFootprint(Project project, ProvFileOpsParamBuilder params)
    throws ProvenanceException {
    if(params.hasFileOpFilters()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.BAD_REQUEST, Level.INFO,
        "footprint should have no predefined file operation filters");
    }
    params.withAggregation(ProvElasticAggregations.ProvAggregations.ARTIFACT_FOOTPRINT);
    return provFileOpsCount(project.getInode().getId(), params.getFileOpsFilterBy(),
      params.getAggregations());
  }
  
  public List<ProvStateMinDTO> provAppFootprintList(Project project, ProvFileOpsParamBuilder params,
    Provenance.FootprintType footprintType)
    throws ProvenanceException {
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
    Map<Long, ProvStateMinDTO> footprint = ProvExecution.processFootprint(searchResult.getItems(), footprintType);
    return new ArrayList<>(footprint.values());
  }
  
  public ProvFileOpDTO.PList provFileOpsList(Project project, ProvFileOpsParamBuilder params)
    throws ProvenanceException {
    if(!params.getAggregations().isEmpty()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
        "aggregations currently only allowed with count");
    }
    ProvFileOpDTO.PList fileOps;
    if(params.hasPagination()) {
      fileOps =  provFileOpsBase(project.getInode().getId(),
        params.getFileOpsFilterBy(), params.getFileOpsSortBy(),
        params.getPagination().getValue0(), params.getPagination().getValue1());
    } else {
      fileOps =  provFileOpsScrolling(project.getInode().getId(), params.getFileOpsFilterBy());
    }
    
    if (params.hasAppExpansion()) {
      //If withAppStates, update params based on appIds of items files and do a appState index query.
      //After this filter the fileStates based on the results of the appState query
      for (ProvOpsElastic fileOp : fileOps.getItems()) {
        Optional<String> appId = getAppId(fileOp);
        if(appId.isPresent()) {
          params.withAppExpansion(appId.get());
        }
      }
      Map<String, Map<Provenance.AppState, ProvAppStateElastic>> appExps
        = appCtrl.provAppState(params.getAppStateFilter());
      Iterator<ProvOpsElastic> fileOpIt = fileOps.getItems().iterator();
      while (fileOpIt.hasNext()) {
        ProvOpsElastic fileOp = fileOpIt.next();
        Optional<String> appId = getAppId(fileOp);
        if(appId.isPresent() && appExps.containsKey(appId.get())) {
          Map<Provenance.AppState, ProvAppStateElastic> appExp = appExps.get(appId.get());
          fileOp.setAppState(ProvAppHelper.buildAppState(appExp));
        } else {
          fileOp.setAppState(ProvAppStateDTO.unknown());
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
    throws ProvenanceException {
    if(!params.getAggregations().isEmpty()) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
        "aggregations currently only allowed with count");
    }
    List<ProvStateMinDTO> fileStates = provAppFootprintList(project, params, footprintType);
    Pair<Map<Long, ProvTree.Builder<ProvStateMinDTO>>,
      Map<Long, ProvTree.Builder<ProvStateMinDTO>>> result
      = treeCtrl.processAsTree(fileStates, () -> new ProvFootprintFileStateTreeElastic(), fullTree);
    return Pair.with((Map<Long, ProvFootprintFileStateTreeElastic>)(Map)result.getValue0(),
      (Map<Long, ProvFootprintFileStateTreeElastic>)(Map)(result.getValue1()));
  }
  
  private Optional<String> getAppId(ProvOpsElastic fileOp) {
    if(fileOp.getAppId().equals("none")) {
      return Optional.empty();
    } else {
      return Optional.of(fileOp.getAppId());
    }
  }
  
  private ProvFileOpDTO.PList provFileOpsBase(Long projectIId,
    Map<String, ProvParser.FilterVal> fileOpsFilters,
    List<Pair<ProvParser.Field, SortOrder>> fileOpsSortBy,
    Integer offset, Integer limit)
    throws ProvenanceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.baseSearchRequest(
        settings.getProvFileIndex(projectIId),
        settings.getElasticDefaultScrollPageSize())
        .andThen(filterByOpsParams(fileOpsFilters))
        .andThen(ElasticHelper.withFileOpsOrder(fileOpsSortBy))
        .andThen(ElasticHelper.withPagination(offset, limit, settings.getElasticMaxScrollPageSize()));
    SearchRequest request = srF.get();
    Pair<Long, List<ProvOpsElastic>> searchResult;
    try {
      searchResult = client.search(request, fileOpsParser());
    } catch (ElasticException e) {
      String msg = "provenance - elastic query problem";
      throw ProvHelper.fromElastic(e, msg, msg + " - file ops");
    }
    return new ProvFileOpDTO.PList(searchResult.getValue1(), searchResult.getValue0());
  }
  
  private ProvFileOpDTO.PList provFileOpsScrolling(Long projectIId,
    Map<String, ProvParser.FilterVal> fileOpsFilters)
    throws ProvenanceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.scrollingSearchRequest(
        settings.getProvFileIndex(projectIId),
        settings.getElasticDefaultScrollPageSize())
        .andThen(filterByOpsParams(fileOpsFilters));
    SearchRequest request = srF.get();
    Pair<Long, List<ProvOpsElastic>> searchResult;
    try {
      searchResult = client.searchScrolling(request, fileOpsParser());
    } catch (ElasticException e) {
      String msg = "provenance - elastic query problem";
      throw ProvHelper.fromElastic(e, msg, msg + " - file ops");
    }
    return new ProvFileOpDTO.PList(searchResult.getValue1(), searchResult.getValue0());
  }
  
  private <S> S provFileOpsScrolling(Long projectIId, Map<String, ProvParser.FilterVal> fileOpsFilters,
    ElasticHitsHandler<?, S, ?, ProvenanceException> proc)
    throws ProvenanceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.scrollingSearchRequest(
        settings.getProvFileIndex(projectIId),
        settings.getElasticDefaultScrollPageSize())
        .andThen(filterByOpsParams(fileOpsFilters));
    SearchRequest request = srF.get();
    Pair<Long, S> searchResult;
    try {
      searchResult = client.searchScrolling(request, proc);
    } catch (ElasticException e) {
      String msg = "provenance - elastic query problem";
      throw ProvHelper.fromElastic(e, msg, msg + " - file ops");
    }
    return searchResult.getValue1();
  }
  
  private ProvFileOpDTO.Count provFileOpsCount(Long projectIId,
    Map<String, ProvParser.FilterVal> fileOpsFilters,
    Set<ProvElasticAggregations.ProvAggregations> aggregations)
    throws ProvenanceException {
    
    Map<ProvElasticAggregations.ProvAggregations, ElasticAggregationParser<?, ProvenanceException>> aggParsers
      = new HashMap<>();
    List<AggregationBuilder> aggBuilders = new ArrayList<>();
    for(ProvElasticAggregations.ProvAggregations aggregation : aggregations) {
      aggParsers.put(aggregation, ProvElasticAggregations.getAggregationParser(aggregation));
      aggBuilders.add(ProvElasticAggregations.getAggregationBuilder(settings, aggregation));
    }
    
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.countSearchRequest(
        settings.getProvFileIndex(projectIId))
        .andThen(filterByOpsParams(fileOpsFilters))
        .andThen(ElasticHelper.withAggregations(aggBuilders));
    SearchRequest request = srF.get();
  
    Pair<Long, Map<ProvElasticAggregations.ProvAggregations, List>> result;
    try {
      result = client.searchCount(request, aggParsers);
    } catch (ElasticException e) {
      String msg = "provenance - elastic query problem";
      throw ProvHelper.fromElastic(e, msg, msg + " - file ops");
    }
    return ProvFileOpDTO.Count.instance(result);
  }
  
  private ElasticHitsHandler<ProvOpsElastic, List<ProvOpsElastic>, ?, ProvenanceException>
    fileOpsParser() {
    return ElasticHitsHandler.instanceAddToList(
      (BasicElasticHit hit) -> ProvOpsElastic.instance(hit));
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> filterByOpsParams(
    Map<String, ProvParser.FilterVal> fileOpsFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvParser.BaseField.ENTRY_TYPE.toString().toLowerCase(),
          ProvParser.EntryType.OPERATION.toString().toLowerCase()));
      query = ElasticHelper.filterByBasicFields(query, fileOpsFilters);
      sr.source().query(query);
      return sr;
    };
  }
  
  public ProvOpsElastic getFileOp(Long projectIId, String docId)
    throws ProvenanceException {
    return ProvOElasticHelper.getFileOp(projectIId, docId, settings, client);
  }
}
