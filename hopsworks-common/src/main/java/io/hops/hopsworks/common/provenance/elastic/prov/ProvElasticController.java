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
package io.hops.hopsworks.common.provenance.elastic.prov;

import io.hops.hopsworks.common.provenance.core.apiToElastic.ProvParser;
import io.hops.hopsworks.common.provenance.core.elastic.BasicElasticHit;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticAggregationParser;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticCache;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticClient;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticHelper;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticHitsHandler;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.ops.ProvElasticAggregations;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpElastic;
import io.hops.hopsworks.common.provenance.state.dto.ProvAppStateElastic;
import io.hops.hopsworks.common.provenance.state.dto.ProvFileStateElastic;
import io.hops.hopsworks.common.provenance.util.functional.CheckedFunction;
import io.hops.hopsworks.common.provenance.util.functional.CheckedSupplier;
import io.hops.hopsworks.common.provenance.state.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpDTO;
import io.hops.hopsworks.common.provenance.state.dto.ProvFileStateDTO;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.exceptions.ServiceException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProvElasticController {
  private static final Logger LOG = Logger.getLogger(ProvElasticController.class.getName());
  @EJB
  private ElasticClient client;
  @EJB
  private ElasticCache cache;
  @EJB
  private Settings settings;
  
  public Map<String, String> mngIndexGetMapping(String index, boolean forceFetch) throws ServiceException {
    return cache.mngIndexGetMapping(index, forceFetch);
  }
  
  public String[] getAllIndices() throws ServiceException {
    String indexRegex = "*" + Settings.PROV_FILE_INDEX_SUFFIX;
    GetIndexRequest request = new GetIndexRequest().indices(indexRegex);
    GetIndexResponse response = client.mngIndexGet(request);
    return response.indices();
  }
  
  public void createProvIndex(Long projectIId) throws ServiceException {
    String indexName = settings.getProvFileIndex(projectIId);
    CreateIndexRequest request = new CreateIndexRequest(indexName);
    client.mngIndexCreate(request);
  }

  public void deleteProvIndex(Long projectIId) throws ServiceException {
    String indexName = settings.getProvFileIndex(projectIId);
    deleteProvIndex(indexName);
  }
  
  public void deleteProvIndex(String indexName) throws ServiceException {
    DeleteIndexRequest request = new DeleteIndexRequest(indexName);
    try {
      DeleteIndexResponse response = client.mngIndexDelete(request);
    } catch (ServiceException e) {
      if(e.getCause() instanceof ElasticsearchException) {
        ElasticsearchException ex = (ElasticsearchException)e.getCause();
        if(ex.status() == RestStatus.NOT_FOUND) {
          LOG.log(Level.INFO, "trying to delete index:{0} - does not exist", indexName);
          return;
        }
      }
      throw e;
    }
  }
  
  public ProvFileStateDTO.PList provFileState(Long projectIId,
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
  
  public Long provFileStateCount(Long projectIId,
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
  
  public Map<String, Map<Provenance.AppState, ProvAppStateElastic>> provAppState(
    Map<String, ProvParser.FilterVal> appStateFilters)
    throws ProvenanceException, ServiceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.scrollingSearchRequest(
        Settings.ELASTIC_INDEX_APP_PROVENANCE,
        Settings.ELASTIC_INDEX_APP_PROVENANCE_DEFAULT_TYPE,
        settings.getElasticDefaultScrollPageSize())
        .andThen(provAppStateQB(appStateFilters));
    SearchRequest request = srF.get();
    Pair<Long, Map<String, Map<Provenance.AppState, ProvAppStateElastic>>> searchResult
      = client.searchScrolling(request, appStateParser());
    return searchResult.getValue1();
  }
  
  private ElasticHitsHandler<ProvFileStateElastic, List<ProvFileStateElastic>, ?, ProvenanceException>
    fileStateParser() {
    return ElasticHitsHandler.instanceAddToList(
      (BasicElasticHit hit) -> ProvFileStateElastic.instance(hit, settings.getHopsRpcTls()));
  }
  
  private ElasticHitsHandler<ProvAppStateElastic, Map<String, Map<Provenance.AppState, ProvAppStateElastic>>, ?,
    ProvenanceException> appStateParser() {
    return ElasticHitsHandler.instanceBasic(new HashMap<>(),
      (BasicElasticHit hit) -> new ProvAppStateElastic(hit),
      (ProvAppStateElastic item, Map<String, Map<Provenance.AppState, ProvAppStateElastic>> state) -> {
        Map<Provenance.AppState, ProvAppStateElastic> appStates =
          state.computeIfAbsent(item.getAppId(), k -> new TreeMap<>());
        appStates.put(item.getAppState(), item);
      });
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
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> filterByOpsParams(
    Map<String, ProvParser.FilterVal> fileOpsFilters, List<Script> scriptFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvParser.BaseField.ENTRY_TYPE.toString().toLowerCase(),
          ProvParser.EntryType.OPERATION.toString().toLowerCase()));
      query = ProvElasticHelper.filterByBasicFields(query, fileOpsFilters);
      query = filterByScripts(query, scriptFilters);
      sr.source().query(query);
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> provAppStateQB(
    Map<String, ProvParser.FilterVal> appStateFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery();
      query = ProvElasticHelper.filterByBasicFields(query, appStateFilters);
      sr.source().query(query);
      return sr;
    };
  }
  
  public ProvFileOpElastic getFileOp(Long projectIId, String docId, boolean soft)
    throws ServiceException, ProvenanceException {
    return ProvElasticHelper.getFileOp(projectIId, docId, soft, settings, client);
  }
  
  private BoolQueryBuilder filterByScripts(BoolQueryBuilder query, List<Script> filterScripts) {
    if(filterScripts.isEmpty()) {
      return query;
    }
    BoolQueryBuilder scriptQB = boolQuery();
    query.must(scriptQB);
    for(Script script : filterScripts) {
      scriptQB.must(new ScriptQueryBuilder(script));
    }
    return query;
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
  
  public ProvFileOpDTO.PList provFileOpsBase(Long projectIId,
    Map<String, ProvParser.FilterVal> fileOpsFilters,
    List<Script> scriptFilter,
    List<Pair<ProvParser.Field, SortOrder>> fileOpsSortBy,
    Integer offset, Integer limit, boolean soft)
    throws ProvenanceException, ServiceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.baseSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE,
        settings.getElasticDefaultScrollPageSize())
        .andThen(filterByOpsParams(fileOpsFilters, scriptFilter))
        .andThen(ElasticHelper.withFileOpsOrder(fileOpsSortBy))
        .andThen(ElasticHelper.withPagination(offset, limit, settings.getElasticMaxScrollPageSize()));
    SearchRequest request = srF.get();
    Pair<Long, List<ProvFileOpElastic>> searchResult
      = client.search(request, fileOpsParser(soft));
    return new ProvFileOpDTO.PList(searchResult.getValue1(), searchResult.getValue0());
  }
  
  public ProvFileOpDTO.PList provFileOpsScrolling(Long projectIId,
    Map<String, ProvParser.FilterVal> fileOpsFilters, List<Script> filterScripts, boolean soft)
    throws ProvenanceException, ServiceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.scrollingSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE,
        settings.getElasticDefaultScrollPageSize())
        .andThen(filterByOpsParams(fileOpsFilters, filterScripts));
    SearchRequest request = srF.get();
    Pair<Long, List<ProvFileOpElastic>> searchResult
      = client.searchScrolling(request, fileOpsParser(soft));
    return new ProvFileOpDTO.PList(searchResult.getValue1(), searchResult.getValue0());
  }
  
  public <S> S provFileOpsScrolling(Long projectIId,
    Map<String, ProvParser.FilterVal> fileOpsFilters, List<Script> filterScripts,
    ElasticHitsHandler<?, S, ?, ProvenanceException> proc)
    throws ProvenanceException, ServiceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.scrollingSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE,
        settings.getElasticDefaultScrollPageSize())
        .andThen(filterByOpsParams(fileOpsFilters, filterScripts));
    SearchRequest request = srF.get();
    Pair<Long, S> searchResult = client.searchScrolling(request, proc);
    return searchResult.getValue1();
  }
  
  public ProvFileOpDTO.Count provFileOpsCount(Long projectIId,
    Map<String, ProvParser.FilterVal> fileOpsFilters,
    List<Script> filterScripts,
    Set<ProvElasticAggregations.ProvAggregations> aggregations)
    throws ServiceException, ProvenanceException {
    
    Map<ProvElasticAggregations.ProvAggregations, ElasticAggregationParser<?, ProvenanceException>> aggParsers
      = new HashMap<>();
    List<AggregationBuilder> aggBuilders = new ArrayList<>();
    for(ProvElasticAggregations.ProvAggregations aggregation : aggregations) {
      aggParsers.put(aggregation, ProvElasticAggregations.getAggregationParser(aggregation));
      aggBuilders.add(ProvElasticAggregations.getAggregationBuilder(settings, aggregation));
    }
    
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.countSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE)
        .andThen(filterByOpsParams(fileOpsFilters, filterScripts))
        .andThen(ElasticHelper.withAggregations(aggBuilders));
    SearchRequest request = srF.get();
    
    Pair<Long, Map<ProvElasticAggregations.ProvAggregations, List>> result = client.searchCount(request, aggParsers);
    return ProvFileOpDTO.Count.instance(result);
  }
  
  private ElasticHitsHandler<ProvFileOpElastic, List<ProvFileOpElastic>, ?, ProvenanceException>
    fileOpsParser(boolean soft) {
    return ElasticHitsHandler.instanceAddToList(
      (BasicElasticHit hit) -> ProvFileOpElastic.instance(hit, soft));
  }
}
