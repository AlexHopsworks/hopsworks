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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.hops.hopsworks.common.util.Ip;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.action.ActionFuture;
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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An Elastic transport client shared by elastic controllers in hopsworks.
 * An Elastic transport client opens quite a few tcp connection that are kept alive for possibly minutes, and in test
 * cases for 50-100 parallel ongoing hopsworks elastic based requests it end up with upwards of 2000 tcp connections
 * used by elastic clients - this can also saturate the amount of tcp connection configured by the elastic node
 * ending up with a NoNodeAvailableException
 */
@Singleton
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
public class HopsworksElasticClient {

  private static final Logger LOG = Logger.getLogger(HopsworksElasticClient.class.getName());
  public static final Integer DEFAULT_PAGE_SIZE = 1000;
  public static final Integer MAX_PAGE_SIZE = 10000;
  public static final Integer ARCHIVAL_PAGE_SIZE = 50;

  @EJB
  private Settings settings;

  private Client elasticClient = null;
  private Cache<String, Map<String, String>> indexMappings;

  @PostConstruct
  private void initClient() {
    try {
      indexMappings = Caffeine.newBuilder()
        .expireAfterWrite(1, TimeUnit.HOURS)
        .maximumSize(50)
        .build();
      getClient();
    } catch (ServiceException ex) {
      LOG.log(Level.SEVERE, null, ex);
    }
  }

  @PreDestroy
  private void closeClient(){
    shutdownClient();
  }

  /**
   * Shuts down the client
   * <p/>
   */
  private void shutdownClient() {
    if (elasticClient != null) {
      elasticClient.close();
      elasticClient = null;
    }
  }
  
  private Client getClient() throws ServiceException {
    if (elasticClient == null) {
      LOG.log(Level.INFO, "creating new elastic client");
      final org.elasticsearch.common.settings.Settings settings
        = org.elasticsearch.common.settings.Settings.builder()
        .put("client.transport.sniff", true) //being able to retrieve other nodes
        .put("cluster.name", "hops").build();
  
      List<String> elasticAddrs = getElasticIpsAsString();
      TransportClient _client = new PreBuiltTransportClient(settings);
      for(String addr : elasticAddrs){
        _client.addTransportAddress(new TransportAddress(
          new InetSocketAddress(addr,
            this.settings.getElasticPort())));
      }
      elasticClient = _client;
      
      Iterator<ThreadPool.Info> tpInfoIt = elasticClient.threadPool().info().iterator();
      StringBuilder tp = new StringBuilder();
      while(tpInfoIt.hasNext()) {
        ThreadPool.Info tpInfo = tpInfoIt.next();
        switch(tpInfo.getName()) {
          case ThreadPool.Names.BULK:
          case ThreadPool.Names.INDEX:
          case ThreadPool.Names.SEARCH:
            tp.append("name:").append(tpInfo.getName())
              .append("(")
              .append(tpInfo.getThreadPoolType())
              .append(",")
              .append(tpInfo.getMin())
              .append(",")
              .append(tpInfo.getMax())
              .append(",")
              .append(tpInfo.getQueueSize().singles())
              .append(")");
        }
      }
      LOG.log(Level.INFO, "threadpools {0}", tp);
    }
    return elasticClient;
  }
  
  private List<String> getElasticIpsAsString() throws ServiceException {
    List<String> addrs = settings.getElasticIps();
    
    for(String addr : addrs) {
      // Validate the ip address pulled from the variables
      if (!Ip.validIp(addr)) {
        try {
          InetAddress.getByName(addr);
        } catch (UnknownHostException ex) {
          throw new ServiceException(
            RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_AVAILABLE,
            Level.SEVERE, null, ex.getMessage(), ex);
        }
      }
    }
    return addrs;
  }
  
  public void cacheMapping(String index, Map<String, String> mapping) {
    indexMappings.put(index, mapping);
  }
  
  public Map<String, String> getMapping(String index) {
    return indexMappings.getIfPresent(index);
  }
  
  public void clearMapping(String index) {
    indexMappings.invalidate(index);
  }
  
  public ActionFuture<GetIndexResponse> mngIndexGet(GetIndexRequest request) throws ServiceException {
    return getClient().admin().indices().getIndex(request);
  }
  
  public ActionFuture<CreateIndexResponse> mngIndexCreate(CreateIndexRequest request) throws ServiceException {
    return getClient().admin().indices().create(request);
  }
  
  public ActionFuture<DeleteIndexResponse> mngIndexDelete(DeleteIndexRequest request) throws ServiceException {
    return getClient().admin().indices().delete(request);
  }
  
  public ActionFuture<GetResponse> get(GetRequest request) throws ServiceException {
    return getClient().get(request);
  }
  
  public ActionFuture<IndexResponse> index(IndexRequest request) throws ServiceException {
    return getClient().index(request);
  }
  public ActionFuture<UpdateResponse> update(UpdateRequest request) throws ServiceException {
    return getClient().update(request);
  }
  public ActionFuture<SearchResponse> search(SearchRequest request) throws ServiceException {
    return getClient().search(request);
  }
  
  public  ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) throws ServiceException {
    return getClient().searchScroll(request);
  }
  
  public ActionFuture<BulkResponse> bulkOp(BulkRequest request) throws ServiceException {
    return getClient().bulk(request);
  }
  
  public void processException(ServiceException ex) {
    LOG.log(Level.WARNING, "elastic client exception", ex);
    shutdownClient();
  }
}

