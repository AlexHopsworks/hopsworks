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
package io.hops.hopsworks.common.elastic;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import io.hops.hopsworks.common.provenance.util.CheckedFunction;
import io.hops.hopsworks.common.provenance.elastic.ProvElasticHelper;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.transport.RemoteTransportException;
import org.javatuples.Pair;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a class made to be used by threads calling the ElasticClient. Methods in here are blocking and will wait
 * for the future responses, which is why we do not include these in the Singleton ElasticClient which contains
 * short, non-blocking methods.
 */
public class HopsworksElasticClientHelper {
  private static final Logger LOG = Logger.getLogger(HopsworksElasticClientHelper.class.getName());
  
  public static GetIndexResponse mngIndexGet(HopsworksElasticClient heClient, GetIndexRequest request)
    throws ServiceException {
    GetIndexResponse response;
    try {
      LOG.log(Level.FINE, "request:{0}", request.toString());
      response = heClient.mngIndexGet(request).get();
    } catch (InterruptedException | ExecutionException e) {
      String msg = "elastic index:" + request.indices() + "error during index get";
      ServiceException se = processException(heClient, e, msg);
      throw se;
    }
    return response;
  }
  
  public static Map<String, Map<String, String>> mngIndexGetMappings(HopsworksElasticClient heClient, String indexRegex)
    throws ServiceException {
    GetIndexRequest request = new GetIndexRequest().indices(indexRegex);
    GetIndexResponse response = mngIndexGet(heClient, request);
  
    Map<String, Map<String, String>> result = new HashMap<>();
    Iterator<ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>>> it1
      = response.mappings().iterator();
    while(it1.hasNext()) {
      ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> current1 = it1.next();
      String index = current1.key;
      Iterator<ObjectObjectCursor<String, MappingMetaData>> it2 = current1.value.iterator();
      while(it2.hasNext()) {
        Map<String, String> mapping = parseMapping((Map)it2.next().value.getSourceAsMap().get("properties"));
        result.put(index, mapping);
      }
    }
    return result;
  }
  
  private static Map<String, String> parseMapping(Map<String, Object> mapping) {
    Map<String, String> result = new HashMap<>();
    for(Map.Entry<String, Object> e1 : mapping.entrySet()) {
      String key1 = e1.getKey();
      Map<String, Object> value = (Map)e1.getValue();
      if(value.containsKey("type")) {
        result.put(key1, (String)value.get("type"));
      } else if(value.containsKey("properties")) {
        Map<String, String> embeddedMapping = parseMapping((Map)value.get("properties"));
        for(Map.Entry<String, String> e2 : embeddedMapping.entrySet()) {
          String key2 = key1 + "." + e2.getKey();
          result.put(key2, e2.getValue());
        }
      }
    }
    return result;
  }
  
  public static CreateIndexResponse mngIndexCreate(HopsworksElasticClient heClient, CreateIndexRequest request)
    throws ServiceException {
    if(request.index().length() > 255) {
      String msg = "elastic index name is too long:" + request.index();
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_ERROR, Level.INFO, msg);
    }
    if(!request.index().equals(request.index().toLowerCase())) {
      String msg = "elastic index names can only contain lower case:" + request.index();
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_ERROR, Level.INFO, msg);
    }
    CreateIndexResponse response;
    try {
      LOG.log(Level.FINE, "request:{0}", request.toString());
      response = heClient.mngIndexCreate(request).get();
    } catch (InterruptedException | ExecutionException e) {
      String msg = "elastic index:" + request.index() + "error during index create";
      ServiceException se = processException(heClient, e, msg);
      throw se;
    }
    if(response.isAcknowledged()) {
      return response;
    } else {
      String msg = "elastic index:" + request.index() + "creation could not be acknowledged";
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_ERROR, Level.INFO, msg);
    }
  }
  
  public static DeleteIndexResponse mngIndexDelete(HopsworksElasticClient heClient, DeleteIndexRequest request)
    throws ServiceException {
    DeleteIndexResponse response;
    try {
      LOG.log(Level.FINE, "request:{0}", request.toString());
      response = heClient.mngIndexDelete(request).get();
    } catch (InterruptedException | ExecutionException e) {
      String msg = "elastic index:" + request.indices()[0] + "error during index delete";
      ServiceException se = processException(heClient, e, msg);
      throw se;
    }
    if(response.isAcknowledged()) {
      return response;
    } else {
      String msg = "elastic index:" + request.indices()[0] + "deletion could not be acknowledged";
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_ERROR, Level.INFO, msg);
    }
  }
  
  public static <S, E extends Exception> S getFileDoc(HopsworksElasticClient heClient, GetRequest request,
    CheckedFunction<Map<String, Object>, S, E> resultParser) throws ServiceException, E {
    GetResponse response;
    try {
      LOG.log(Level.FINE, "request:{0}", request.toString());
      response = heClient.get(request).get();
    } catch (InterruptedException | ExecutionException e) {
      String msg =  "error during get doc:" + request.id();
      ServiceException se = processException(heClient, e, msg);
      throw se;
    }
    if(response.isExists()) {
      return resultParser.apply(response.getSource());
    } else {
      return null;
    }
  }
  
  public static void indexDoc(HopsworksElasticClient heClient, IndexRequest request) throws ServiceException {
    IndexResponse response;
    
    try {
      LOG.log(Level.FINE, "request:{0}", request.toString());
      response = heClient.index(request).get();
    } catch (InterruptedException | ExecutionException e) {
      String msg = "error during index doc:" + request.id();
      ServiceException se = processException(heClient, e, msg);
      throw se;
    }
    if (response.status().getStatus() != 201) {
      String msg = "doc index - bad status response:" + response.status().getStatus();
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_ERROR, Level.INFO, msg);
    }
  }
  
  public static void updateDoc(HopsworksElasticClient heClient, UpdateRequest request) throws ServiceException {
    UpdateResponse response;
    
    try {
      LOG.log(Level.FINE, "request:{0}", request.toString());
      response = heClient.update(request).get();
    } catch (InterruptedException | ExecutionException e) {
      String msg = "error during update doc:" + request.id();
      ServiceException se = processException(heClient, e, msg);
      throw se;
    }
    if (response.status().getStatus() != 200) {
      String msg = "doc update - bad status response:" + response.status().getStatus();
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_ERROR, Level.INFO, msg);
    }
  }
  
  public static <H> Pair<H, Long> searchBasic(HopsworksElasticClient heClient, SearchRequest request,
    ProvElasticHelper.ElasticBasicResultProcessor<H> resultProcessor)
    throws ServiceException, ProvenanceException {
    SearchResponse response;
    LOG.log(Level.FINE, "request:{0}", request.toString());
    response = searchBasicInt(heClient, request);
    resultProcessor.apply(response.getHits().getHits());
    Pair<H, Long> result = Pair.with(resultProcessor.get(), response.getHits().getTotalHits());
    return result;
  }
  
  public static <S> Pair<S, Long> searchScrollingWithBasicAction(HopsworksElasticClient heClient, SearchRequest request,
    ProvElasticHelper.ElasticBasicResultProcessor<S> resultProcessor)
    throws ServiceException, ProvenanceException {
    SearchResponse response;
    long leftover;
    LOG.log(Level.FINE, "request:{0}", request.toString());
    response = searchBasicInt(heClient, request);
    if(response.getHits().getTotalHits() > HopsworksElasticClient.MAX_PAGE_SIZE) {
      String msg = "Elasticsearch query items size is too big: " + response.getHits().getTotalHits();
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_ERROR, Level.INFO, msg);
    }
    long totalHits = response.getHits().totalHits;
    leftover = totalHits - response.getHits().getHits().length;
    resultProcessor.apply(response.getHits().getHits());
    
    while (leftover > 0) {
      SearchScrollRequest next = nextScrollPage(response.getScrollId());
      response = searchScrollingInt(heClient, next);
      leftover = leftover - response.getHits().getHits().length;
      resultProcessor.apply(response.getHits().getHits());
    }
    return Pair.with(resultProcessor.get(), totalHits);
  }
  
  public static <S> S searchScrollingWithComplexAction(HopsworksElasticClient heClient, SearchRequest request,
    ProvElasticHelper.ElasticComplexResultProcessor<S> resultProcessor)
    throws ServiceException, ProvenanceException {
    SearchResponse response;
    long leftover;
    LOG.log(Level.FINE, "request:{0}", request.toString());
    response = searchBasicInt(heClient, request);
    if(response.getHits().getTotalHits() > HopsworksElasticClient.MAX_PAGE_SIZE) {
      String msg = "Elasticsearch query items size is too big: " + response.getHits().getTotalHits();
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_ERROR, Level.INFO, msg);
    }
    leftover = response.getHits().totalHits - response.getHits().getHits().length;
    resultProcessor.apply(response.getHits().getHits());
    
    while (leftover > 0) {
      SearchScrollRequest next = nextScrollPage(response.getScrollId());
      LOG.log(Level.FINE, "request:{0}", next.toString());
      response = searchScrollingInt(heClient, next);
      leftover = leftover - response.getHits().getHits().length;
      resultProcessor.apply(response.getHits().getHits());
    }
    return resultProcessor.get();
  }
  
  private static SearchResponse searchScrollingInt(HopsworksElasticClient heClient, SearchScrollRequest request)
    throws ServiceException {
    SearchResponse response;
    try {
      response = heClient.searchScroll(request).get();
    } catch (InterruptedException | ExecutionException e) {
      String msg = "error querying elastic";
      ServiceException se = processException(heClient, e, msg);
      throw se;
    }
    if (response.status().getStatus() != 200) {
      String msg = "searchBasic query - bad status response:" + response.status().getStatus();
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_ERROR, Level.INFO, msg);
    }
    return response;
  }
  
  private static SearchScrollRequest nextScrollPage(String scrollId) {
    SearchScrollRequest ssr = new SearchScrollRequest(scrollId);
    ssr.scroll(TimeValue.timeValueMinutes(1));
    return ssr;
  }
  
  public static Pair<Long, List<Pair<ProvElasticHelper.ProvAggregations, List>>> searchCount(
    HopsworksElasticClient heClient, SearchRequest request, List<ProvElasticHelper.ProvAggregations> aggregations)
    throws ServiceException, ProvenanceException {
    SearchResponse response;
    LOG.log(Level.FINE, "request:{0}", request.toString());
    response = searchBasicInt(heClient, request);
    LOG.log(Level.FINE, "response:{0}", response.toString());
    if(aggregations.isEmpty()) {
      return Pair.with(response.getHits().getTotalHits(), Collections.emptyList());
    } else {
      List<Pair<ProvElasticHelper.ProvAggregations, List>> aggResults = new LinkedList<>();
      for (ProvElasticHelper.ProvAggregations aggregation : aggregations) {
        aggResults.add(Pair.with(aggregation, aggregation.parser.apply(response.getAggregations())));
      }
      return Pair.with(response.getHits().getTotalHits(), aggResults);
    }
  }
  
  public static void bulkDelete(HopsworksElasticClient heClient, BulkRequest request)
    throws ServiceException {
    BulkResponse response;
    try {
      LOG.log(Level.FINE, "request:{0}", request.toString());
      response = heClient.bulkOp(request).get();
    } catch (InterruptedException | ExecutionException e) {
      String msg = "error during bulk delete";
      ServiceException se = processException(heClient, e, msg);
      throw se;
    }
    if(response.hasFailures()) {
      String msg = "failures during bulk delete";
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_ERROR, Level.INFO, msg);
    }
  }
  
  private static SearchResponse searchBasicInt(HopsworksElasticClient heClient, SearchRequest request)
    throws ServiceException {
    SearchResponse response;
    try {
      response = heClient.search(request).get();
    } catch (ExecutionException | InterruptedException e) {
      String msg = "error querying elastic index";
      ServiceException se = processException(heClient, e, msg);
      throw se;
    }
    if (response.status().getStatus() != 200) {
      String msg = "searchBasic query - bad status response:" + response.status().getStatus();
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_ERROR, Level.INFO, msg);
    }
    return response;
  }
  
  private static ServiceException processException(HopsworksElasticClient heClient, Exception e, String msg) {
    if(e.getCause() instanceof RemoteTransportException) {
      RemoteTransportException e1 = (RemoteTransportException)e.getCause();
      if(e1.getCause() instanceof SearchPhaseExecutionException) {
        SearchPhaseExecutionException e2 = (SearchPhaseExecutionException)e1.getCause();
        if(e2.getCause() instanceof QueryShardException) {
          QueryShardException e3 = (QueryShardException) e2.getCause();
          if (e3.getMessage().startsWith("No mapping found for ")) {
            int idx1 = e3.getMessage().indexOf("[");
            int idx2 = e3.getMessage().indexOf("]");
            if (idx1 != -1 && idx2 != -1 && idx1 < idx2) {
              String field = e3.getMessage().substring(idx1 + 1, idx2);
              String devMsg = "index[" + e1.getIndex().getName() + "] - " +
                "error querying - index missing mapping for field[" + field + "]";
              ServiceException ex = new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_NO_MAPPING,
                Level.INFO, msg, devMsg, e);
              LOG.log(Level.INFO, devMsg);
              return ex;
            }
          }
        }
      }
    }
    LOG.log(Level.WARNING, msg, e);
    ServiceException ex = new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVICE_ERROR, Level.WARNING,
      msg, e.getMessage(), e);
    heClient.processException(ex);
    return ex;
  }
  
  public static class ElasticPairException extends Exception {
    private ServiceException e1 = null;
    private GenericException e2 = null;
    
    private ElasticPairException(ServiceException e1, GenericException e2) {
      super();
      this.e1 = e1;
      this.e2 = e2;
    }
    
    public static ElasticPairException instanceService(ServiceException e1) {
      return new ElasticPairException(e1, null);
    }
    
    public  static ElasticPairException instanceGeneric(GenericException e2) {
      return new ElasticPairException(null, e2);
    }
    
    public void check() throws ServiceException, GenericException {
      if(e1 != null) {
        throw e1;
      }
      if(e2 != null) {
        throw e2;
      }
    }
  }
}
