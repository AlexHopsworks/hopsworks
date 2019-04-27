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

import io.hops.hopsworks.common.provenance.util.CheckedFunction;
import io.hops.hopsworks.common.provenance.v2.ProvElasticFields;
import io.hops.hopsworks.common.provenance.v2.ProvFileOps;
import io.hops.hopsworks.common.provenance.v2.xml.FileOp;
import io.hops.hopsworks.common.provenance.v2.xml.FileOpDTO;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

public class ProvElasticHelper {
  private static final Logger LOG = Logger.getLogger(HopsworksElasticClient.class.getName());
  
  public static class ElasticBasicResultProcessor<S> {
    S accumulator;
    CheckedBiConsumer<SearchHit[], S, GenericException> base;
    
    public ElasticBasicResultProcessor(S accumulator, CheckedBiConsumer<SearchHit[], S, GenericException> base) {
      this.base = base;
      this.accumulator = accumulator;
    }
    
    public void apply(SearchHit[] searchHits) throws GenericException {
      base.accept(searchHits, accumulator);
    }
    
    public S get() {
      return accumulator;
    }
  }
  
  public static class ElasticComplexResultProcessor<S> {
    S accumulator;
    CheckedBiConsumer<SearchHit[], S, HopsworksElasticClientHelper.ElasticPairException> base;
  
    public ElasticComplexResultProcessor(S accumulator,
      CheckedBiConsumer<SearchHit[], S, HopsworksElasticClientHelper.ElasticPairException> base) {
      this.base = base;
      this.accumulator = accumulator;
    }
  
    public void apply(SearchHit[] searchHits) throws GenericException, ServiceException {
      try {
        base.accept(searchHits, accumulator);
      } catch (HopsworksElasticClientHelper.ElasticPairException e) {
        //throw inner exception;
        e.check();
      }
    }
    
    public S get() {
      return accumulator;
    }
  }
  
  public static QueryBuilder fullTextSearch(String key, String term) {
    return boolQuery()
      .should(matchPhraseQuery(key, term.toLowerCase()))
      .should(prefixQuery(key, term.toLowerCase()))
      .should(fuzzyQuery(key, term.toLowerCase()))
      .should(wildcardQuery(key, String.format("*%s*", term.toLowerCase())));
  }
  
  public enum FilterByScripts {
    ;
    public final Script script;
    
    FilterByScripts(Script script) {
      this.script = script;
    }
  }
  
  public enum ProvAggregations {
    FILES_IN(
      filesInABuilder("files_in", ProvElasticFields.FileBase.INODE_ID),
      filesInAParser("files_in")),
    FILES_LEAST_ACTIVE_BY_LAST_ACCESSED(
      leastActiveByLastAccessedABuilder(
        "files_least_active_by_last_accessed", ProvElasticFields.FileBase.INODE_ID,
        "lastOpTimestamp", ProvElasticFields.FileOpsBase.TIMESTAMP),
      leastActiveByLastAccessedAParser("files_least_active_by_last_accessed", "lastOpTimestamp")),
    PROJECTS_LEAST_ACTIVE_BY_LAST_ACCESSED(
      leastActiveByLastAccessedABuilder(
        "projects_least_active_by_last_accessed", ProvElasticFields.FileBase.PROJECT_I_ID,
        "lastOpTimestamp", ProvElasticFields.FileOpsBase.TIMESTAMP),
      leastActiveByLastAccessedAParser("projects_least_active_by_last_accessed", "lastOpTimestamp")),
    ARTIFACT_FOOTPRINT(appArtifactFootprintABuilder(), appArtifactFootprintAParser());
    
    public final AggregationBuilder aggregation;
    public final CheckedFunction<Aggregations, List, GenericException> parser;
    ProvAggregations(AggregationBuilder aggregation, CheckedFunction<Aggregations, List, GenericException> parser) {
      this.aggregation = aggregation;
      this.parser = parser;
    }
  }
  
  private static AggregationBuilder filesInABuilder(String agg, ProvElasticFields.Field aggField) {
    return AggregationBuilders.terms(agg)
      .field(aggField.toString())
      .size(HopsworksElasticClient.DEFAULT_PAGE_SIZE);
  }
  
  private static CheckedFunction<Aggregations, List, GenericException> filesInAParser(String agg1Name) {
    return (Aggregations aggregations) -> {
      List<FileOpDTO.FileAggregation> result = new LinkedList<>();
      Terms agg1 = aggregations.get(agg1Name);
      if (agg1 == null) {
        return null;
      }
      List<? extends Terms.Bucket> buckets = agg1.getBuckets();
      for(Terms.Bucket bucket : buckets) {
        Long inodeId = (Long)bucket.getKeyAsNumber();
        result.add(new FileOpDTO.FileAggregation(inodeId, bucket.getDocCount()));
      }
      return result;
    };
  }
  
  private static AggregationBuilder leastActiveByLastAccessedABuilder(String agg1, ProvElasticFields.Field agg1Field,
    String agg2, ProvElasticFields.Field agg2Field) {
    return AggregationBuilders.terms(agg1)
      .field(agg1Field.toString())
      .size(HopsworksElasticClient.DEFAULT_PAGE_SIZE)
      .order(InternalOrder.aggregation(agg2, true))
      .subAggregation(
        AggregationBuilders.max(agg2).field(agg2Field.toString()));
  }
  
  private static CheckedFunction<Aggregations, List, GenericException> leastActiveByLastAccessedAParser(
    String agg1Name, String agg2Name) {
    return (Aggregations aggregations) -> {
      List<FileOpDTO.FileAggregation> result = new LinkedList<>();
      Terms agg1 = aggregations.get(agg1Name);
      if(agg1 == null) {
        return result;
      }
      List<? extends Terms.Bucket> agg1Buckets = agg1.getBuckets();
      for(Terms.Bucket bucket : agg1Buckets) {
        Max agg2 = bucket.getAggregations().get(agg2Name);
        Long inodeId = (Long)bucket.getKeyAsNumber();
        Long lastAccessed = ((Number) agg2.getValue()).longValue();
        result.add(new FileOpDTO.FileAggregation(inodeId, bucket.getDocCount(), lastAccessed));
      }
      return result;
    };
  }
  
  private static CheckedFunction<Aggregations, List, GenericException> appArtifactFootprintAParser() {
    return (Aggregations aggregations) -> {
      List<FileOpDTO.Artifact> result = new LinkedList<>();
      Terms artifacts = aggregations.get("artifacts");
      if(artifacts == null) {
        return result;
      }
      for(Terms.Bucket artifactBucket : artifacts.getBuckets()) {
        FileOpDTO.Artifact artifact = new FileOpDTO.Artifact();
  
        Terms files = artifactBucket.getAggregations().get("files");
        if(files == null) {
          continue;
        }
        result.add(artifact);
        
        FileOpDTO.ArtifactBase base = null;
        for(Terms.Bucket fileBucket : artifacts.getBuckets()) {
          FileOpDTO.ArtifactFile file = new FileOpDTO.ArtifactFile();
          //create
          Filter createFilter = fileBucket.getAggregations().get("create");
          if(createFilter != null) {
            TopHits createOpHits = createFilter.getAggregations().get("create_op");
            if(createOpHits.getHits().getTotalHits() > 1) {
              throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.WARNING,
                "cannot have two create ops on the same inode");
            }
            FileOp createOp = FileOp.instance(createOpHits.getHits().getAt(0), false);
            file.addCreate(createOp);
            base = extractBaseIfNotExists(base, createOp);
          }
  
          //delete
          Filter deleteFilter = fileBucket.getAggregations().get("delete");
          if(deleteFilter != null) {
            TopHits deleteOpHits = deleteFilter.getAggregations().get("delete_op");
            if(deleteOpHits.getHits().getTotalHits() > 1) {
              throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.WARNING,
                "cannot have two delete ops on the same inode");
            }
            FileOp deleteOp = FileOp.instance(deleteOpHits.getHits().getAt(0), false);
            file.addDelete(deleteOp);
            base = extractBaseIfNotExists(base, deleteOp);
          }
  
          //read
          Filter readFilter = fileBucket.getAggregations().get("read");
          if(readFilter != null) {
            TopHits readOpHits = readFilter.getAggregations().get("first_read");
            FileOp readOp = FileOp.instance(readOpHits.getHits().getAt(0), false);
            file.addFirstRead(readOp, readOpHits.getHits().getTotalHits());
            base = extractBaseIfNotExists(base, readOp);
          }
  
          //append
          Filter appendFilter = fileBucket.getAggregations().get("append");
          if(appendFilter != null) {
            TopHits appendOpHits = appendFilter.getAggregations().get("first_append");
            FileOp appendOp = FileOp.instance(appendOpHits.getHits().getAt(0), false);
            file.addFirstAppend(appendOp, appendOpHits.getHits().getTotalHits());
            base = extractBaseIfNotExists(base, appendOp);
          }
          artifact.addComponent(file);
        }
        artifact.setBase(base);
      }
      return result;
    };
  }
  
  private static FileOpDTO.ArtifactBase extractBaseIfNotExists(FileOpDTO.ArtifactBase base, FileOp op)
    throws GenericException {
    if(base != null) {
      return base;
    }
    ProvElasticFields.MLType mlType = ProvElasticFields.parseMLType(op.getMlType());
    return new FileOpDTO.ArtifactBase(op.getProjectInodeId(), op.getDatasetInodeId(), op.getMlId(), mlType);
  }
  
  private static AggregationBuilder appArtifactFootprintABuilder() {
    return
      AggregationBuilders.terms("artifacts")
        .field(ProvElasticFields.FileOpsAux.ML_ID.toString())
        .subAggregation(
          AggregationBuilders.terms("files")
            .field(ProvElasticFields.FileBase.INODE_ID.toString())
            .subAggregation(AggregationBuilders
              .filter("create", termQuery(ProvElasticFields.FileOpsBase.INODE_OPERATION.toString(),
                  ProvFileOps.CREATE.toString()))
              .subAggregation(AggregationBuilders
                .topHits("create_op")
                .sort(ProvElasticFields.FileOpsBase.TIMESTAMP.toString(), SortOrder.ASC)
                .size(1)))
            .subAggregation(AggregationBuilders
              .filter("delete", termQuery(ProvElasticFields.FileOpsBase.INODE_OPERATION.toString(),
                ProvFileOps.ACCESS_DATA.toString()))
              .subAggregation(AggregationBuilders
                .topHits("delete_op")
                .sort(ProvElasticFields.FileOpsBase.TIMESTAMP.toString(), SortOrder.ASC)
                .size(1)))
            .subAggregation(AggregationBuilders
              .filter("read", termQuery(ProvElasticFields.FileOpsBase.INODE_OPERATION.toString(),
                  ProvFileOps.ACCESS_DATA.toString()))
              .subAggregation(AggregationBuilders
                .topHits("first_read")
                .sort(ProvElasticFields.FileOpsBase.TIMESTAMP.toString(), SortOrder.ASC)
                .size(1)))
            .subAggregation(AggregationBuilders
              .filter("append", termQuery(ProvElasticFields.FileOpsBase.INODE_OPERATION.toString(),
                  ProvFileOps.ACCESS_DATA.toString()))
              .subAggregation(AggregationBuilders
                .topHits("first_append")
                .sort(ProvElasticFields.FileOpsBase.TIMESTAMP.toString(), SortOrder.ASC)
                .size(1))));
  }
}
