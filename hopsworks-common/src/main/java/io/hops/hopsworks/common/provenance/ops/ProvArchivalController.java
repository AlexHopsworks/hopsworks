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

import com.google.gson.Gson;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.dao.hdfs.inode.Inode;
import io.hops.hopsworks.common.dao.hdfs.inode.InodeFacade;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.common.hdfs.Utils;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.core.apiToElastic.ProvParser;
import io.hops.hopsworks.common.provenance.core.elastic.BasicElasticHit;
import io.hops.hopsworks.common.provenance.core.elastic.ProvElasticController;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticHelper;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticHitsHandler;
import io.hops.hopsworks.common.provenance.util.ProvElasticHelper;
import io.hops.hopsworks.common.provenance.ops.apiToElastic.ProvOParser;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpElastic;
import io.hops.hopsworks.common.provenance.ops.dto.ProvArchiveDTO;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpDTO;
import io.hops.hopsworks.common.provenance.util.functional.CheckedFunction;
import io.hops.hopsworks.common.provenance.util.functional.CheckedSupplier;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ElasticException;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

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
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProvArchivalController {
  private static final Logger LOG = Logger.getLogger(ProvArchivalController.class.getName());
  @EJB
  private Settings settings;
  @EJB
  private ProvElasticController client;
  @EJB
  private ProvOpsController ppoClient;
  @EJB
  private DistributedFsService dfs;
  @EJB
  private ProjectFacade projectFacade;
  @EJB
  private InodeFacade inodeFacade;
  
  public static class Archival {
    Long projectIId;
    int counter = 0;
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
  
  public interface Store {
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
  
  public String archiveId(Long inodeId) {
    return inodeId + "-archive";
  }
  
  public ProvArchiveDTO.Base getArchiveDoc(Long projectIId, Long inodeId) throws ElasticException, ProvenanceException {
    GetRequest request = new GetRequest(
      settings.getProvFileIndex(projectIId),
      archiveId(inodeId));
    ProvArchiveDTO.Base result = client.getDoc(request,
      (BasicElasticHit hit) -> ProvArchiveDTO.Base.instance(hit));
    if(result == null) {
      result = new ProvArchiveDTO.Base();
      result.setInodeId(inodeId);
    }
    return result;
  }
  
  private void createArchivalEntry(Long projectIId, Long inodeId)
    throws ProvenanceException, ElasticException {
    GetRequest getBase = new GetRequest(
      settings.getProvFileIndex(projectIId),
      archiveId(inodeId));
    ProvArchiveDTO.Base baseArchival = client.getDoc(getBase,
      (BasicElasticHit hit) -> ProvArchiveDTO.Base.instance(hit));
    if(baseArchival == null) {
      IndexRequest indexBase = new IndexRequest(settings.getProvFileIndex(projectIId));
      indexBase.id(archiveId(inodeId));
      Map<String, Object> docMap = new HashMap<>();
      docMap.put(ProvParser.BaseField.INODE_ID.toString(), inodeId);
      docMap.put(ProvParser.BaseField.ENTRY_TYPE.toString(),
        ProvParser.EntryType.ARCHIVE.toString().toLowerCase());
      indexBase.source(docMap);
      client.indexDoc(indexBase);
    }
  }
  
  private int provArchiveFilePrefix(ProvArchivalController.Archival archivalState,
    Map<String, ProvParser.FilterVal> fileOpsFilters, Optional<String> skipDocO)
    throws ElasticException, ProvenanceException {
    SearchRequest archivalRequest = archivalScrollingRequest(archivalState.projectIId, fileOpsFilters, skipDocO);
    client.searchScrolling(archivalRequest, archivalHandler(archivalState));
    return archivalState.counter;
  }
  
  private SearchRequest archivalScrollingRequest(Long projectIId,
    Map<String, ProvParser.FilterVal> fileOpsFilters, Optional<String> skipDoc)
    throws ProvenanceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.scrollingSearchRequest(
        settings.getProvFileIndex(projectIId),
        settings.getProvElasticArchivalPageSize())
        .andThen(filterByArchival(fileOpsFilters, skipDoc))
        .andThen(ElasticHelper.sortByTimestamp(SortOrder.ASC));
    return srF.get();
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> filterByArchival(
    Map<String, ProvParser.FilterVal> fileOpsFilters, Optional<String> skipDoc) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvParser.BaseField.ENTRY_TYPE.toString().toLowerCase(),
          ProvParser.EntryType.OPERATION.toString().toLowerCase()));
      query = ProvElasticHelper.filterByBasicFields(query, fileOpsFilters);
      if(skipDoc.isPresent()) {
        query.mustNot(idsQuery().addIds(skipDoc.get()));
      }
      sr.source().query(query);
      return sr;
    };
  }
  
  public int provArchiveFilePrefix(Long projectIId, Long inodeId, Optional<Long> timestamp,
    ProvArchivalController.Archival archival)
    throws ProvenanceException, ElasticException {
    createArchivalEntry(projectIId, inodeId);
    Map<String, ProvParser.FilterVal> fileOpsFilters = new HashMap<>();
    ProvParser.addToFilters(fileOpsFilters, Pair.with(ProvOParser.FileOps.FILE_I_ID, inodeId));
    if(timestamp.isPresent()) {
      ProvParser.addToFilters(fileOpsFilters, Pair.with(ProvOParser.FileOpsAux.TIMESTAMP_LTE, timestamp));
    }
    return provArchiveFilePrefix(archival, fileOpsFilters, Optional.empty());
  }
  
  private int provCleanupFilePrefix(Long inodeId, Optional<Long> timestamp,
    Optional<String> skipDocO, ProvArchivalController.Archival archival)
    throws ProvenanceException, ElasticException {
    Map<String, ProvParser.FilterVal> fileOpsFilters = new HashMap<>();
    ProvParser.addToFilters(fileOpsFilters, Pair.with(ProvOParser.FileOps.FILE_I_ID, inodeId));
    if(timestamp.isPresent()) {
      ProvParser.addToFilters(fileOpsFilters, Pair.with(ProvOParser.FileOpsAux.TIMESTAMP_LTE, timestamp.get()));
    }
    return provArchiveFilePrefix(archival, fileOpsFilters, skipDocO);
  }
  
  public int provCleanupFilePrefix(Long inodeId, Optional<Long> timestamp,
    ProvArchivalController.Archival archival)
    throws ProvenanceException, ElasticException {
    return provCleanupFilePrefix(inodeId, timestamp, Optional.empty(), archival);
  }
  
  public int provCleanupFilePrefix(Long projectIId, String docId, boolean skipDoc,
    ProvArchivalController.Archival archival)
    throws ElasticException, ProvenanceException {
    ProvFileOpElastic doc = ProvElasticHelper.getFileOp(projectIId, docId, settings, client);
    if(doc.getInodeId() == null) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.ARCHIVAL_STORE, Level.INFO,
        "problem parsing field: file inode id");
    }
    Optional<String> skipDocO = skipDoc ? Optional.of(docId) : Optional.empty();
    return provCleanupFilePrefix(doc.getInodeId(), Optional.of(doc.getTimestamp()), skipDocO, archival);
  }
  
  public ElasticHitsHandler<ProvFileOpElastic, Archival, ?, ProvenanceException> archivalHandler(
    Archival archivalState) {
    return ElasticHitsHandler.instanceWithAction(archivalState,
      (BasicElasticHit hit) -> ProvFileOpElastic.instance(hit),
      (ProvFileOpElastic item, Archival state) -> state.pendingOps.add(item),
      (Archival state) -> {
        BulkRequest bulkDelete = new BulkRequest();
        for(ProvFileOpElastic item : state.pendingOps) {
          state.store.addOp(item);
          DeleteRequest req = new DeleteRequest(settings.getProvFileIndex(state.projectIId), item.getId());
          bulkDelete.add();
        }
        Long line = state.store.save();
        try {
          if (state.baseDoc.isPresent()) {
            updateArchive(state.projectIId, state.baseDoc.get(), state.store.getLocation(), line);
          }
          client.bulkDelete(bulkDelete);
        } catch(ElasticException e) {
          throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.ARCHIVAL_STORE, Level.WARNING,
            "error while archiving file", "archiving error", e);
        }
        state.incBy(state.pendingOps.size());
        state.pendingOps.clear();
      });
  }

  private void updateArchive(Long projectIId, Long inodeId, String location, Long line)
    throws ElasticException {
    UpdateRequest updateBase = new UpdateRequest(
      settings.getProvFileIndex(projectIId),
      archiveId(inodeId));
    Map<String, Object> updateMap = new HashMap<>();
    updateMap.put(ProvOParser.BaseField.ARCHIVE_LOC.toString(), new String[]{location + ":" + line});
    updateBase.doc(updateMap);
    client.updateDoc(updateBase);
  }
  
  //
  private ProvFileOpDTO.PList cleanupFiles(Project project, Integer limit, Long beforeTimestamp)
    throws ProvenanceException, ElasticException {
    ProvFileOpsParamBuilder params = new ProvFileOpsParamBuilder()
      .filterByField(ProvOParser.FileOps.PROJECT_I_ID, project.getInode().getId().toString())
      .filterByFileOperation(Provenance.FileOps.DELETE)
      .filterByField(ProvOParser.FileOpsAux.TIMESTAMP_LT, beforeTimestamp.toString())
      .sortByField(ProvOParser.BaseField.TIMESTAMP, SortOrder.ASC)
      .withPagination(0, limit);
    return ppoClient.provFileOpsList(project, params);
  }
  
  private ProvFileOpDTO.Count cleanupFilesSize(Project project, Integer limit, Long beforeTimestamp)
    throws ProvenanceException, ElasticException {
    ProvFileOpsParamBuilder params = new ProvFileOpsParamBuilder()
      .filterByField(ProvOParser.FileOps.PROJECT_I_ID, project.getInode().getId().toString())
      .filterByFileOperation(Provenance.FileOps.DELETE)
      .filterByField(ProvOParser.FileOpsAux.TIMESTAMP_LT, beforeTimestamp.toString())
      .withPagination(0, limit);
    return ppoClient.provFileOpsCount(project, params);
  }
  
  public ProvArchiveDTO.Round cleanupRound(Project project, Integer limit)
    throws ProvenanceException, ElasticException {
    Long beforeTimestamp = System.currentTimeMillis() - (settings.getProvArchiveDelay() * 1000);
    return cleanupRound(project, limit, beforeTimestamp);
  }
  
  public ProvArchiveDTO.Round cleanupRound(Project project, Integer limit, Long beforeTimestamp)
    throws ProvenanceException, ElasticException {
    Long cleaned = 0l;
    for(ProvFileOpElastic fileOp : cleanupFiles(project, limit, beforeTimestamp).getItems()) {
      Long projectIId = project.getInode().getId();
      ProvArchivalController.Archival archival
        = new ProvArchivalController.Archival(projectIId, new ProvArchivalController.NoStore(), Optional.empty());
      cleaned += provCleanupFilePrefix(fileOp.getInodeId(), Optional.empty(), archival);
      if(cleaned > limit) {
        break;
      }
    }
    return new ProvArchiveDTO.Round(0l, cleaned);
  }
  
  public ProvArchiveDTO.Round provCleanupFilePrefix(Project project, Long inodeId, Long timestamp)
    throws ProvenanceException, ElasticException {
    Long projectIId = project.getInode().getId();
    ProvArchivalController.Archival archival
      = new ProvArchivalController.Archival(projectIId, new ProvArchivalController.NoStore(), Optional.empty());
    int cleaned = provCleanupFilePrefix(inodeId, Optional.of(timestamp), archival);
    return new ProvArchiveDTO.Round(0l, (long)cleaned);
  }
  
  public ProvArchiveDTO.Round provCleanupFilePrefix(Project project, Long inodeId)
    throws ProvenanceException, ElasticException {
    Long projectIId = project.getInode().getId();
    ProvArchivalController.Archival archival
      = new ProvArchivalController.Archival(projectIId, new ProvArchivalController.NoStore(), Optional.empty());
    int cleaned = provCleanupFilePrefix(inodeId, Optional.empty(), archival);
    return new ProvArchiveDTO.Round(0l, (long)cleaned);
  }
  
  public ProvArchiveDTO.Round provCleanupFilePrefix(Project project, String docId, boolean skipDoc)
    throws ElasticException, ProvenanceException {
    Long projectIId = project.getInode().getId();
    ProvArchivalController.Archival archival
      = new ProvArchivalController.Archival(projectIId, new ProvArchivalController.NoStore(), Optional.empty());
    int cleaned = provCleanupFilePrefix(project.getInode().getId(), docId, skipDoc, archival);
    return new ProvArchiveDTO.Round(0l, (long)cleaned);
  }
  
  public ProvFileOpDTO.Count cleanupSize(Project project) throws ProvenanceException, ElasticException {
    Long beforeTimestamp = System.currentTimeMillis() - ( settings.getProvArchiveDelay() * 1000);
    ProvFileOpDTO.Count result = cleanupFilesSize(project, settings.getProvArchiveSize(), beforeTimestamp);
    return result;
  }
  
  public Pair<ProvArchiveDTO.Round, String> archiveRound(String nextToCheck, Integer limitIdx, Integer limitOps)
    throws ProvenanceException, ElasticException {
    Long beforeTimestamp = System.currentTimeMillis() - ( settings.getProvArchiveDelay() * 1000);
    return archiveRound(nextToCheck, limitIdx, limitOps, beforeTimestamp);
  }
  
  public Pair<ProvArchiveDTO.Round, String> archiveRound(String nextToCheck, Integer limitIdx, Integer limitOps,
    Long beforeTimestamp)
    throws ProvenanceException, ElasticException {
    String[] indices = getAllIndices();
    
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
        LOG.log(Level.INFO, "deleting prov index:{0} with no corresponding project", indexName);
        deleteProvIndex(indexName);
        cleaned++;
        continue;
      }
      DistributedFileSystemOps dfso = dfs.getDfsOps();
      try {
        for (ProvFileOpElastic fileOp : cleanupFiles(project, limitOps, beforeTimestamp).getItems()) {
          ProvArchivalController.Store store
            = new ProvArchivalController.Hops(Utils.getProjectPath(fileOp.getProjectName()), dfso);
          store.init();
          Long projectIId = project.getInode().getId();
          ProvArchivalController.Archival archival
            = new ProvArchivalController.Archival(projectIId, store, Optional.of(fileOp.getInodeId()));
          archived += provArchiveFilePrefix(project.getInode().getId(), fileOp.getInodeId(),
            Optional.empty(), archival);
          if (archived > limitOps) {
            break;
          }
        }
      } finally {
        if(dfso != null) {
          dfs.closeDfsClient(dfso);
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
    throws ProvenanceException, ElasticException {
    Long beforeTimestamp = System.currentTimeMillis() - settings.getProvArchiveDelay();
    return projectArchiveRound(project, limit, beforeTimestamp);
  }
  
  public ProvArchiveDTO.Round projectArchiveRound(Project project, Integer limit, Long beforeTimestamp)
    throws ProvenanceException, ElasticException {
    Long archived = 0L;
    DistributedFileSystemOps dfso = dfs.getDfsOps();
    try {
      for(ProvFileOpElastic fileOp : cleanupFiles(project, limit, beforeTimestamp).getItems()) {
        ProvArchivalController.Store store
          = new ProvArchivalController.Hops(Utils.getProjectPath(fileOp.getProjectName()), dfso);
        store.init();
        Long projectIId = project.getInode().getId();
        ProvArchivalController.Archival archival
          = new ProvArchivalController.Archival(projectIId, store, Optional.of(fileOp.getInodeId()));
        archived += provArchiveFilePrefix(project.getInode().getId(), fileOp.getInodeId(), Optional.empty(),
          archival);
      }
    } finally {
      if(dfso != null) {
        dfs.closeDfsClient(dfso);
      }
    }
    return new ProvArchiveDTO.Round(archived, 0l);
  }
  
  private String[] getAllIndices() throws ElasticException {
    String indexRegex = "*" + Settings.PROV_FILE_INDEX_SUFFIX;
    GetIndexRequest request = new GetIndexRequest(indexRegex);
    GetIndexResponse response = client.mngIndexGet(request);
    return response.getIndices();
  }
  
  private void deleteProvIndex(String indexName) throws ElasticException {
    DeleteIndexRequest request = new DeleteIndexRequest(indexName);
    try {
      AcknowledgedResponse response = client.mngIndexDelete(request);
    } catch (ElasticException e) {
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
  
  public int archiveOps(Project project, int limitOps, long beforeTimestamp)
    throws ProvenanceException, ElasticException {
    DistributedFileSystemOps dfso = dfs.getDfsOps();
    int archived = 0;
    try {
      for (ProvFileOpElastic fileOp : cleanupFiles(project, limitOps, beforeTimestamp).getItems()) {
        ProvArchivalController.Store store
          = new ProvArchivalController.Hops(Utils.getProjectPath(fileOp.getProjectName()), dfso);
        store.init();
        Long projectIId = project.getInode().getId();
        ProvArchivalController.Archival archival
          = new ProvArchivalController.Archival(projectIId, store, Optional.of(fileOp.getInodeId()));
        archived += provArchiveFilePrefix(project.getInode().getId(), fileOp.getInodeId(),
          Optional.empty(), archival);
        if (archived > limitOps) {
          break;
        }
      }
      return archived;
    } finally {
      if(dfso != null) {
        dfs.closeDfsClient(dfso);
      }
    }
  }
}
