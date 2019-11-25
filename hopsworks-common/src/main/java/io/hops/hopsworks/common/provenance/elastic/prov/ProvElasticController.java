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

import com.google.gson.Gson;
import io.hops.hopsworks.common.provenance.elastic.core.BasicElasticHit;
import io.hops.hopsworks.common.provenance.elastic.core.ElasticAggregationParser;
import io.hops.hopsworks.common.provenance.elastic.core.ElasticClient;
import io.hops.hopsworks.common.provenance.elastic.core.ElasticHelper;
import io.hops.hopsworks.common.provenance.elastic.core.ElasticHitsHandler;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.util.CheckedFunction;
import io.hops.hopsworks.common.provenance.util.CheckedSupplier;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvElasticFields;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvFileQuery;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvParamBuilder;
import io.hops.hopsworks.common.provenance.xml.ProvArchiveDTO;
import io.hops.hopsworks.common.provenance.xml.ProvFileOpDTO;
import io.hops.hopsworks.common.provenance.xml.ProvFileStateDTO;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProvElasticController {
  private static final Logger LOG = Logger.getLogger(ProvElasticController.class.getName());
  @EJB
  private ElasticClient client;
  @EJB
  private ProvElasticCache cache;
  @EJB
  private DistributedFsService dfs;
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
    CreateIndexResponse response = client.mngIndexCreate(request);
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
    Map<String, ProvFileQuery.FilterVal> fileStateFilters,
    List<Pair<ProvFileQuery.Field, SortOrder>> fileStateSortBy,
    Map<String, String> xAttrsFilters, Map<String, String> likeXAttrsFilters, Set<String> hasXAttrsFilters,
    List<ProvFileStateParamBuilder.SortE> xattrSortBy,
    Integer offset, Integer limit)
    throws ProvenanceException, ServiceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      baseSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE)
        .andThen(filterByStateParams(fileStateFilters, xAttrsFilters, likeXAttrsFilters, hasXAttrsFilters))
        .andThen(withFileStateOrder(fileStateSortBy, xattrSortBy))
        .andThen(withPagination(offset, limit));
    SearchRequest request = srF.get();
    Pair<Long, List<ProvFileStateElastic>> searchResult = client.search(request, fileStateParser());
    return new ProvFileStateDTO.PList(searchResult.getValue1(), searchResult.getValue0());
  }
  
  public Long provFileStateCount(Long projectIId,
    Map<String, ProvFileQuery.FilterVal> fileStateFilters,
    Map<String, String> xAttrsFilters, Map<String, String> likeXAttrsFilters, Set<String> hasXAttrsFilters)
    throws ProvenanceException, ServiceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      countSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE)
        .andThen(filterByStateParams(fileStateFilters, xAttrsFilters, likeXAttrsFilters, hasXAttrsFilters));
    SearchRequest request = srF.get();
    Long searchResult = client.searchCount(request);
    return searchResult;
  }
  
  public ProvFileOpDTO.PList provFileOpsBase(Long projectIId,
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters,
    List<Script> scriptFilter,
    List<Pair<ProvFileQuery.Field, SortOrder>> fileOpsSortBy,
    Integer offset, Integer limit, boolean soft)
    throws ProvenanceException, ServiceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      baseSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE)
        .andThen(filterByOpsParams(fileOpsFilters, scriptFilter))
        .andThen(withFileOpsOrder(fileOpsSortBy))
        .andThen(withPagination(offset, limit));
    SearchRequest request = srF.get();
    Pair<Long, List<ProvFileOpElastic>> searchResult
      = client.search(request, fileOpsParser(soft));
    return new ProvFileOpDTO.PList(searchResult.getValue1(), searchResult.getValue0());
  }
  
  public ProvFileOpDTO.PList provFileOpsScrolling(Long projectIId,
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters, List<Script> filterScripts, boolean soft)
    throws ProvenanceException, ServiceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      scrollingSearchRequest(
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
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters, List<Script> filterScripts,
    ElasticHitsHandler<?, S, ?, ProvenanceException> proc)
    throws ProvenanceException, ServiceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      scrollingSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE,
        settings.getElasticDefaultScrollPageSize())
        .andThen(filterByOpsParams(fileOpsFilters, filterScripts));
    SearchRequest request = srF.get();
    Pair<Long, S> searchResult = client.searchScrolling(request, proc);
    return searchResult.getValue1();
  }
  
  public ProvFileOpDTO.Count provFileOpsCount(Long projectIId,
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters,
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
      countSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE)
        .andThen(filterByOpsParams(fileOpsFilters, filterScripts))
        .andThen(withAggregations(aggBuilders));
    SearchRequest request = srF.get();
    
    Pair<Long, Map<ProvElasticAggregations.ProvAggregations, List>> result = client.searchCount(request, aggParsers);
    return ProvFileOpDTO.Count.instance(result);
  }
  
  public Map<String, Map<Provenance.AppState, ProvAppStateElastic>> provAppState(
    Map<String, ProvFileQuery.FilterVal> appStateFilters)
    throws ProvenanceException, ServiceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      scrollingSearchRequest(
        Settings.ELASTIC_INDEX_APP_PROVENANCE,
        Settings.ELASTIC_INDEX_APP_PROVENANCE_DEFAULT_TYPE,
        settings.getElasticDefaultScrollPageSize())
        .andThen(provAppStateQB(appStateFilters));
    SearchRequest request = srF.get();
    Pair<Long, Map<String, Map<Provenance.AppState, ProvAppStateElastic>>> searchResult
      = client.searchScrolling(request, appStateParser());
    return searchResult.getValue1();
  }
  
  //*** Archival
  private static class Archival {
    Long projectIId;
    Long counter = 0l;
    List<ProvFileOpElastic> pendingOps = new ArrayList<>();
    Store store;
    Optional<Long> baseDoc;
    
    public Archival(Long projectIId, Store store, Optional<Long> baseArchiveDoc) {
      this.projectIId = projectIId;
      this.store = store;
      this.baseDoc = baseArchiveDoc;
    }
    
    void incBy(int val) {
      counter += val;
    }
  }
  
  private interface Store {
    void init() throws ProvenanceException;
    void addOp(ProvFileOpElastic op) throws ProvenanceException;
    Long save() throws ProvenanceException;
    String getLocation();
  }
  
  public static class NoStore implements Store {
  
    @Override
    public void init() {
    }
  
    @Override
    public void addOp(ProvFileOpElastic op) {
    }
  
    @Override
    public Long save()  {
      return 0l;
    }
  
    @Override
    public String getLocation() {
      return "no_store";
    }
  }
  public static class Hops implements Store {
    String projectPath;
    String relativePath = "Resources/provenance";
    String archiveFile = "archive";
    String statusFile = "status";
    Long line = 0l;
    DistributedFileSystemOps dfso;
    ProvFileOpDTO.File file;
    
    public Hops(String projectPath, DistributedFileSystemOps dfso) {
      this.projectPath = projectPath;
      this.dfso = dfso;
    }
    
    @Override
    public void init() throws ProvenanceException {
      String dirPath;
      if(projectPath.endsWith("/")){
        dirPath = projectPath + relativePath;
      } else {
        dirPath = projectPath + "/" + relativePath;
      }
      
      String filePath = dirPath + "/" + archiveFile;
      String statusPath = dirPath + "/" + statusFile;
      try {
        if(!dfso.exists(dirPath)) {
          dfso.mkdir(dirPath);
        } else if(!dfso.isDir(dirPath)) {
          throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.ARCHIVAL_STORE, Level.WARNING,
            "the provenance dir cannot be created - a file with its name already exists");
        }
        if(!dfso.exists(filePath)) {
          try(FSDataOutputStream out = dfso.create(filePath)){
          }
        }
        if(!dfso.exists(statusPath)) {
          try(FSDataOutputStream out = dfso.create(statusPath)){
            out.writeLong(line);
            out.flush();
          }
        } else {
          try(FSDataInputStream in = dfso.open(statusPath)){
            line = in.readLong();
          }
        }
      } catch (IOException e) {
        throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.ARCHIVAL_STORE, Level.WARNING,
          "error while saving file for archival on hops");
      }
    }
    
    @Override
    public void addOp(ProvFileOpElastic op) throws ProvenanceException {
      if(file == null) {
        file = new ProvFileOpDTO.File(op);
      } else {
        file.addOp(op);
      }
    }
    
    @Override
    public Long save() throws ProvenanceException {
      String dirPath = projectPath + "/" + relativePath;
      String filePath = dirPath + "/" + archiveFile;
      String statusPath = dirPath + "/" + statusFile;
      try(FSDataOutputStream archive = dfso.append(filePath)) {
        if(!dfso.exists(projectPath)) {
          LOG.log(Level.INFO, "project:{0} does not exist anymore - can't use for archive", projectPath);
          throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.ARCHIVAL_STORE, Level.WARNING,
            "archival project does not exist anymore");
        }
        Gson gson = new Gson();
        String data = gson.toJson(file) + "\n";
        archive.writeBytes(data);
        archive.flush();
        line++;
        dfso.rm(new Path(statusPath), false);
        try(FSDataOutputStream status = dfso.create(statusPath)){
          status.writeLong(line);
          status.flush();
        }
        file = null;
        return line;
      } catch(IOException e) {
        throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.ARCHIVAL_STORE, Level.WARNING,
          "error while archiving file on the fs - hops", "hops exception", e);
      }
    }
    
    @Override
    public String getLocation() {
      String dirPath = projectPath + "/" + relativePath;
      String filePath = dirPath + "/" + archiveFile;
      return filePath;
    }
    
  }
  
  private ElasticHitsHandler<ProvFileOpElastic, Archival, ?, ProvenanceException> archivalHandler(
    Archival archivalState) {
    return ElasticHitsHandler.instanceWithAction(archivalState,
      (BasicElasticHit hit) -> ProvFileOpElastic.instance(hit, true),
      (ProvFileOpElastic item, Archival state) -> state.pendingOps.add(item),
      (Archival state) -> {
        BulkRequest bulkDelete = new BulkRequest();
        for(ProvFileOpElastic item : state.pendingOps) {
          state.store.addOp(item);
          DeleteRequest req
            = new DeleteRequest(settings.getProvFileIndex(state.projectIId), Settings.PROV_FILE_DOC_TYPE, item.getId());
          bulkDelete.add();
        }
        Long line = state.store.save();
        try {
          if (state.baseDoc.isPresent()) {
            updateArchive(state.projectIId, state.baseDoc.get(), state.store.getLocation(), line);
          }
          client.bulkDelete(bulkDelete);
        } catch(ServiceException e) {
          throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.ARCHIVAL_STORE, Level.WARNING,
            "error while archiving file", "archiving error", e);
        }
        state.incBy(state.pendingOps.size());
        state.pendingOps.clear();
      });
  }
  
  public Long provArchiveFilePrefix(Long projectIId, Long inodeId, Optional<Long> timestamp,
    String withArchiveProject)
    throws ProvenanceException, ServiceException {
    createArchive(projectIId, inodeId);
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters = new HashMap<>();
    ProvParamBuilder.addToFilters(fileOpsFilters, Pair.with(ProvFileQuery.FileOps.FILE_I_ID, inodeId));
    if(timestamp.isPresent()) {
      ProvParamBuilder.addToFilters(fileOpsFilters, Pair.with(ProvFileQuery.FileOpsAux.TIMESTAMP_LTE, timestamp));
    }
    DistributedFileSystemOps dfso = dfs.getDfsOps();
    Store store = new Hops(withArchiveProject, dfso);
    store.init();
    Archival archival = new Archival(projectIId, store, Optional.of(inodeId));
    return provArchiveFilePrefix(archival, fileOpsFilters, Optional.empty());
  }
  
  public Long provCleanupFilePrefix(Long projectIId, Long inodeId, Optional<Long> timestamp)
    throws ProvenanceException, ServiceException {
    return provCleanupFilePrefix(projectIId, inodeId, timestamp, Optional.empty());
  }
  
  public Long provCleanupFilePrefix(Long projectIId, String docId, boolean skipDoc)
    throws ServiceException, ProvenanceException {
    ProvFileOpElastic doc = getFileOp(projectIId, docId, true);
    if(doc.getInodeId() == null) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.ARCHIVAL_STORE, Level.INFO,
        "problem parsing field: file inode id");
    }
    Optional<String> skipDocO = skipDoc ? Optional.of(docId) : Optional.empty();
    return provCleanupFilePrefix(projectIId, doc.getInodeId(), Optional.of(doc.getTimestamp()), skipDocO);
  }
  
  private Long provCleanupFilePrefix(Long projectIId, Long inodeId, Optional<Long> timestamp,
    Optional<String> skipDocO)
    throws ProvenanceException, ServiceException {
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters = new HashMap<>();
    ProvParamBuilder.addToFilters(fileOpsFilters, Pair.with(ProvFileQuery.FileOps.FILE_I_ID, inodeId));
    if(timestamp.isPresent()) {
      ProvParamBuilder.addToFilters(fileOpsFilters, Pair.with(ProvFileQuery.FileOpsAux.TIMESTAMP_LTE, timestamp.get()));
    }
    Archival archival = new Archival(projectIId, new NoStore(), Optional.empty());
    return provArchiveFilePrefix(archival, fileOpsFilters, skipDocO);
  }
  
  public void provArchiveProject(Long projectIId, String docId, boolean skipDoc)
    throws ServiceException, ProvenanceException {
    ProvFileOpElastic doc = getFileOp(projectIId, docId, true);
    Optional<String> skipDocO = skipDoc ? Optional.of(docId) : Optional.empty();
    throw new NotImplementedException();
  }
  
  private Long provArchiveFilePrefix(Archival archivalState, Map<String, ProvFileQuery.FilterVal> fileOpsFilters,
    Optional<String> skipDocO)
    throws ServiceException, ProvenanceException {
    SearchRequest archivalRequest = archivalScrollingRequest(archivalState.projectIId, fileOpsFilters, skipDocO);
    client.searchScrolling(archivalRequest, archivalHandler(archivalState));
    return archivalState.counter;
  }
  
  private SearchRequest archivalScrollingRequest(Long projectIId,
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters, Optional<String> skipDoc)
    throws ProvenanceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      scrollingSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE,
        settings.getProvElasticArchivalPageSize())
      .andThen(filterByArchival(fileOpsFilters, skipDoc))
      .andThen(sortByTimestamp());
    return srF.get();
  }
  
  public String archiveId(Long inodeId) {
    return inodeId + "-archive";
  }
  
  public ProvArchiveDTO.Base getArchive(Long projectIId, Long inodeId) throws ServiceException, ProvenanceException {
    GetRequest request = new GetRequest(
      settings.getProvFileIndex(projectIId),
      Settings.PROV_FILE_DOC_TYPE,
      archiveId(inodeId));
    ProvArchiveDTO.Base result = client.getDoc(request,
      (BasicElasticHit hit) -> ProvArchiveDTO.Base.instance(hit));
    if(result == null) {
      result = new ProvArchiveDTO.Base();
      result.setInodeId(inodeId);
    }
    return result;
  }
  
  private void createArchive(Long projectIId, Long inodeId)
    throws ProvenanceException, ServiceException {
    GetRequest getBase = new GetRequest(
      settings.getProvFileIndex(projectIId),
      Settings.PROV_FILE_DOC_TYPE,
      archiveId(inodeId));
    ProvArchiveDTO.Base baseArchival = client.getDoc(getBase,
      (BasicElasticHit hit) -> ProvArchiveDTO.Base.instance(hit));
    if(baseArchival == null) {
      IndexRequest indexBase = new IndexRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE,
        archiveId(inodeId));
      Map<String, Object> docMap = new HashMap<>();
      docMap.put(ProvElasticFields.FileBase.INODE_ID.toString(), inodeId);
      docMap.put(ProvElasticFields.FileBase.ENTRY_TYPE.toString(),
        ProvElasticFields.EntryType.ARCHIVE.toString().toLowerCase());
      indexBase.source(docMap);
      client.indexDoc(indexBase);
    }
  }
  
  private void updateArchive(Long projectIId, Long inodeId, String location, Long line)
    throws ServiceException {
    UpdateRequest updateBase = new UpdateRequest(
      settings.getProvFileIndex(projectIId),
      Settings.PROV_FILE_DOC_TYPE,
      archiveId(inodeId));
    Map<String, Object> updateMap = new HashMap<>();
    updateMap.put(ProvElasticFields.FileOpsBase.ARCHIVE_LOC.toString(), new String[]{location + ":" + line});
    updateBase.doc(updateMap);
    client.updateDoc(updateBase);
  }
  //****
  
  public ProvFileOpElastic getFileOp(Long projectIId, String docId, boolean soft)
    throws ServiceException, ProvenanceException {
    GetRequest request = new GetRequest(
      settings.getProvFileIndex(projectIId),
      Settings.PROV_FILE_DOC_TYPE,
      docId);
    ProvFileOpElastic result =  client.getDoc(request,
      (BasicElasticHit hit) -> ProvFileOpElastic.instance(hit, soft));
    return result;
  }
  
  private CheckedSupplier<SearchRequest, ProvenanceException> baseSearchRequest(String index, String docType) {
    return () -> {
      SearchRequest sr = new SearchRequest(index)
        .types(docType);
      sr.source().size(settings.getElasticDefaultScrollPageSize());
      return sr;
    };
  }
  
  private CheckedSupplier<SearchRequest, ProvenanceException> scrollingSearchRequest(String index, String docType,
    int pageSize) {
    return () -> {
      SearchRequest sr = new SearchRequest(index)
        .types(docType)
        .scroll(TimeValue.timeValueMinutes(1));
      sr.source().size(pageSize);
      return sr;
    };
  }
  
  private CheckedSupplier<SearchRequest, ProvenanceException> countSearchRequest(String index, String docType) {
    return () -> {
      SearchRequest sr = new SearchRequest(index)
        .types(docType);
      sr.source().size(0);
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> withPagination(
    Integer offset, Integer limit) {
    return (SearchRequest sr) -> {
      try {
        ElasticHelper.checkPagination(settings, offset, limit);
      } catch(ServiceException e) {
        throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.BAD_REQUEST, Level.INFO,
          "query with malformed pagination", "query with malformed pagination", e);
      }
      if(offset != null) {
        sr.source().from(offset);
      }
      if(limit != null) {
        sr.source().size(limit);
      }
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> withFileStateOrder(
    List<Pair<ProvFileQuery.Field, SortOrder>> fileStateSortBy, List<ProvFileStateParamBuilder.SortE> xattrSortBy) {
    return (SearchRequest sr) -> {
      //      if(fileStateSortBy.isEmpty() && xattrSortBy.isEmpty()) {
      //        srb.addSort(SortBuilders.fieldSort("_doc").order(SortOrder.ASC));
      //      } else {
      for (Pair<ProvFileQuery.Field, SortOrder> sb : fileStateSortBy) {
        sr.source().sort(SortBuilders.fieldSort(sb.getValue0().elasticFieldName()).order(sb.getValue1()));
      }
      for (ProvFileStateParamBuilder.SortE sb : xattrSortBy) {
        sr.source().sort(SortBuilders.fieldSort(sb.key).order(sb.order));
      }
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> withFileOpsOrder(
    List<Pair<ProvFileQuery.Field, SortOrder>> fileOpsSortBy) {
    return (SearchRequest sr) -> {
      for (Pair<ProvFileQuery.Field, SortOrder> sb : fileOpsSortBy) {
        sr.source().sort(SortBuilders.fieldSort(sb.getValue0().elasticFieldName()).order(sb.getValue1()));
      }
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> sortByTimestamp() {
    return (SearchRequest sr) -> {
      sr.source().sort(ProvElasticFields.FileOpsBase.TIMESTAMP.toString().toLowerCase(), SortOrder.ASC);
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> withAggregations(
    List<AggregationBuilder> aggregationBuilder) {
    return (SearchRequest sr) -> {
      if(!aggregationBuilder.isEmpty()) {
        for (AggregationBuilder builder : aggregationBuilder) {
          sr.source().aggregation(builder);
        }
      }
      return sr;
    };
  }
  
  private ElasticHitsHandler<ProvFileStateElastic, List<ProvFileStateElastic>, ?, ProvenanceException>
    fileStateParser() {
    return ElasticHitsHandler.instanceAddToList(
      (BasicElasticHit hit) -> ProvFileStateElastic.instance(hit, settings.getHopsRpcTls()));
  }
  
  private ElasticHitsHandler<ProvFileOpElastic, List<ProvFileOpElastic>, ?, ProvenanceException>
    fileOpsParser(boolean soft) {
    return ElasticHitsHandler.instanceAddToList(
      (BasicElasticHit hit) -> ProvFileOpElastic.instance(hit, soft));
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
    Map<String, ProvFileQuery.FilterVal> fileStateFilters,
    Map<String, String> xAttrsFilters, Map<String, String> likeXAttrsFilters, Set<String> hasXAttrsFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvElasticFields.FileBase.ENTRY_TYPE.toString().toLowerCase(),
          ProvElasticFields.EntryType.STATE.toString().toLowerCase()));
      query = filterByBasicFields(query, fileStateFilters);
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
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters, List<Script> scriptFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvElasticFields.FileBase.ENTRY_TYPE.toString().toLowerCase(),
          ProvElasticFields.EntryType.OPERATION.toString().toLowerCase()));
      query = filterByBasicFields(query, fileOpsFilters);
      query = filterByScripts(query, scriptFilters);
      sr.source().query(query);
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> filterByArchival(
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters, Optional<String> skipDoc) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvElasticFields.FileBase.ENTRY_TYPE.toString().toLowerCase(),
          ProvElasticFields.EntryType.OPERATION.toString().toLowerCase()));
      query = filterByBasicFields(query, fileOpsFilters);
      if(skipDoc.isPresent()) {
        query.mustNot(idsQuery().addIds(skipDoc.get()));
      }
      sr.source().query(query);
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> provAppStateQB(
    Map<String, ProvFileQuery.FilterVal> appStateFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery();
      query = filterByBasicFields(query, appStateFilters);
      sr.source().query(query);
      return sr;
    };
  }
  
  private BoolQueryBuilder filterByBasicFields(BoolQueryBuilder query,
    Map<String, ProvFileQuery.FilterVal> filters) throws ProvenanceException {
    for (Map.Entry<String, ProvFileQuery.FilterVal> fieldFilters : filters.entrySet()) {
      query.must(fieldFilters.getValue().query());
    }
    return query;
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
}
