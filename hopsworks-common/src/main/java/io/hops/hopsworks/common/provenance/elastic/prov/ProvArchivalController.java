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
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvElasticFields;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvFileQuery;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvParamBuilder;
import io.hops.hopsworks.common.provenance.elastic.core.BasicElasticHit;
import io.hops.hopsworks.common.provenance.elastic.core.ElasticClient;
import io.hops.hopsworks.common.provenance.elastic.core.ElasticHelper;
import io.hops.hopsworks.common.provenance.elastic.core.ElasticHitsHandler;
import io.hops.hopsworks.common.provenance.util.CheckedFunction;
import io.hops.hopsworks.common.provenance.util.CheckedSupplier;
import io.hops.hopsworks.common.provenance.xml.ProvArchiveDTO;
import io.hops.hopsworks.common.provenance.xml.ProvFileOpDTO;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
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
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProvArchivalController {
  private static final Logger LOG = Logger.getLogger(ProvElasticController.class.getName());
  @EJB
  private Settings settings;
  @EJB
  private ElasticClient client;
  
  public static class Archival {
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
  
  private void createArchivalEntry(Long projectIId, Long inodeId)
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
  
  public void provArchiveProject(Long projectIId, String docId, boolean skipDoc)
    throws ServiceException, ProvenanceException {
//    ProvFileOpElastic doc = getFileOp(projectIId, docId, true);
//    Optional<String> skipDocO = skipDoc ? Optional.of(docId) : Optional.empty();
    throw new NotImplementedException();
  }
  
  private Long provArchiveFilePrefix(ProvArchivalController.Archival archivalState,
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters, Optional<String> skipDocO)
    throws ServiceException, ProvenanceException {
    SearchRequest archivalRequest = archivalScrollingRequest(archivalState.projectIId, fileOpsFilters, skipDocO);
    client.searchScrolling(archivalRequest, archivalHandler(archivalState));
    return archivalState.counter;
  }
  
  private SearchRequest archivalScrollingRequest(Long projectIId,
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters, Optional<String> skipDoc)
    throws ProvenanceException {
    CheckedSupplier<SearchRequest, ProvenanceException> srF =
      ElasticHelper.scrollingSearchRequest(
        settings.getProvFileIndex(projectIId),
        Settings.PROV_FILE_DOC_TYPE,
        settings.getProvElasticArchivalPageSize())
        .andThen(filterByArchival(fileOpsFilters, skipDoc))
        .andThen(ElasticHelper.sortByTimestamp(SortOrder.ASC));
    return srF.get();
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, ProvenanceException> filterByArchival(
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters, Optional<String> skipDoc) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvElasticFields.FileBase.ENTRY_TYPE.toString().toLowerCase(),
          ProvElasticFields.EntryType.OPERATION.toString().toLowerCase()));
      query = ProvElasticHelper.filterByBasicFields(query, fileOpsFilters);
      if(skipDoc.isPresent()) {
        query.mustNot(idsQuery().addIds(skipDoc.get()));
      }
      sr.source().query(query);
      return sr;
    };
  }
  
  public Long provArchiveFilePrefix(Long projectIId, Long inodeId, Optional<Long> timestamp,
    ProvArchivalController.Archival archival)
    throws ProvenanceException, ServiceException {
    createArchivalEntry(projectIId, inodeId);
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters = new HashMap<>();
    ProvParamBuilder.addToFilters(fileOpsFilters, Pair.with(ProvFileQuery.FileOps.FILE_I_ID, inodeId));
    if(timestamp.isPresent()) {
      ProvParamBuilder.addToFilters(fileOpsFilters, Pair.with(ProvFileQuery.FileOpsAux.TIMESTAMP_LTE, timestamp));
    }
    return provArchiveFilePrefix(archival, fileOpsFilters, Optional.empty());
  }
  
  private Long provCleanupFilePrefix(Long projectIId, Long inodeId, Optional<Long> timestamp,
    Optional<String> skipDocO, ProvArchivalController.Archival archival)
    throws ProvenanceException, ServiceException {
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters = new HashMap<>();
    ProvParamBuilder.addToFilters(fileOpsFilters, Pair.with(ProvFileQuery.FileOps.FILE_I_ID, inodeId));
    if(timestamp.isPresent()) {
      ProvParamBuilder.addToFilters(fileOpsFilters, Pair.with(ProvFileQuery.FileOpsAux.TIMESTAMP_LTE, timestamp.get()));
    }
    return provArchiveFilePrefix(archival, fileOpsFilters, skipDocO);
  }
  
  public Long provCleanupFilePrefix(Long projectIId, Long inodeId, Optional<Long> timestamp,
    ProvArchivalController.Archival archival)
    throws ProvenanceException, ServiceException {
    return provCleanupFilePrefix(projectIId, inodeId, timestamp, Optional.empty(), archival);
  }
  
  public Long provCleanupFilePrefix(Long projectIId, String docId, boolean skipDoc,
    ProvArchivalController.Archival archival)
    throws ServiceException, ProvenanceException {
    ProvFileOpElastic doc = ProvElasticHelper.getFileOp(projectIId, docId, true, settings, client);
    if(doc.getInodeId() == null) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.ARCHIVAL_STORE, Level.INFO,
        "problem parsing field: file inode id");
    }
    Optional<String> skipDocO = skipDoc ? Optional.of(docId) : Optional.empty();
    return provCleanupFilePrefix(projectIId, doc.getInodeId(), Optional.of(doc.getTimestamp()), skipDocO, archival);
  }
  
  public ElasticHitsHandler<ProvFileOpElastic, Archival, ?, ProvenanceException> archivalHandler(
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
}
