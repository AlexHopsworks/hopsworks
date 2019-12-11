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

import io.hops.hopsworks.common.provenance.core.apiToElastic.ProvParser;
import io.hops.hopsworks.common.provenance.core.elastic.BasicElasticHit;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticAggregation;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticAggregationParser;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.ops.apiToElastic.ProvOParser;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpElastic;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpDTO;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.restutils.RESTCodes;
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

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class ProvElasticAggregations {
  private static final Logger LOG = Logger.getLogger(ProvElasticAggregations.class.getName());
  
  public enum ProvAggregations implements ElasticAggregation {
    FILES_IN,
    FILES_LEAST_ACTIVE_BY_LAST_ACCESSED,
    PROJECTS_LEAST_ACTIVE_BY_LAST_ACCESSED,
    ARTIFACT_FOOTPRINT;
  }
  
  public static AggregationBuilder getAggregationBuilder(Settings settings, ProvAggregations agg)
    throws ProvenanceException {
    switch(agg) {
      case FILES_IN:
        return filesInABuilder(settings, "files_in", ProvParser.BaseField.INODE_ID);
      case FILES_LEAST_ACTIVE_BY_LAST_ACCESSED:
        return leastActiveByLastAccessedABuilder(settings,
          "files_least_active_by_last_accessed", ProvParser.BaseField.INODE_ID,
          "lastOpTimestamp", ProvOParser.BaseField.TIMESTAMP);
      case PROJECTS_LEAST_ACTIVE_BY_LAST_ACCESSED:
        return leastActiveByLastAccessedABuilder(settings,
          "projects_least_active_by_last_accessed", ProvParser.BaseField.PROJECT_I_ID,
          "lastOpTimestamp", ProvOParser.BaseField.TIMESTAMP);
      case ARTIFACT_FOOTPRINT:
        return appArtifactFootprintABuilder();
      default:
        throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.INTERNAL_ERROR, Level.WARNING,
          "unknown aggregation" + agg);
    }
  }
  
  public static ElasticAggregationParser<?, ProvenanceException> getAggregationParser(ProvAggregations agg)
    throws ProvenanceException {
    switch(agg) {
      case FILES_IN:
        return filesInAParser("files_in");
      case FILES_LEAST_ACTIVE_BY_LAST_ACCESSED:
        return leastActiveByLastAccessedAParser(
          "files_least_active_by_last_accessed",
          "lastOpTimestamp");
      case PROJECTS_LEAST_ACTIVE_BY_LAST_ACCESSED:
        return leastActiveByLastAccessedAParser(
          "projects_least_active_by_last_accessed",
          "lastOpTimestamp");
      case ARTIFACT_FOOTPRINT:
        return appArtifactFootprintAParser();
      default:
        throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.INTERNAL_ERROR, Level.WARNING,
          "unknown aggregation" + agg);
    }
  }
  
  private static AggregationBuilder filesInABuilder(Settings settings, String agg, ProvParser.ElasticField aggField) {
    return AggregationBuilders.terms(agg)
      .field(aggField.toString())
      .size(settings.getElasticDefaultScrollPageSize());
  }
  
  private static ElasticAggregationParser<ProvFileOpDTO.FileAggregation, ProvenanceException> filesInAParser(
    String agg1Name) {
    return (Aggregations aggregations) -> {
      List<ProvFileOpDTO.FileAggregation> result = new LinkedList<>();
      Terms agg1 = aggregations.get(agg1Name);
      if (agg1 == null) {
        return null;
      }
      List<? extends Terms.Bucket> buckets = agg1.getBuckets();
      for(Terms.Bucket bucket : buckets) {
        Long inodeId = (Long)bucket.getKeyAsNumber();
        result.add(new ProvFileOpDTO.FileAggregation(inodeId, bucket.getDocCount()));
      }
      return result;
    };
  }
  
  private static AggregationBuilder leastActiveByLastAccessedABuilder(Settings settings,
    String agg1, ProvParser.ElasticField agg1Field,
    String agg2, ProvParser.ElasticField agg2Field) {
    return AggregationBuilders.terms(agg1)
      .field(agg1Field.toString())
      .size(settings.getElasticDefaultScrollPageSize())
      .order(InternalOrder.aggregation(agg2, true))
      .subAggregation(
        AggregationBuilders.max(agg2).field(agg2Field.toString()));
  }
  
  private static ElasticAggregationParser<ProvFileOpDTO.FileAggregation, ProvenanceException>
    leastActiveByLastAccessedAParser(String agg1Name, String agg2Name) {
    return (Aggregations aggregations) -> {
      List<ProvFileOpDTO.FileAggregation> result = new LinkedList<>();
      Terms agg1 = aggregations.get(agg1Name);
      if(agg1 == null) {
        return result;
      }
      List<? extends Terms.Bucket> agg1Buckets = agg1.getBuckets();
      for(Terms.Bucket bucket : agg1Buckets) {
        Max agg2 = bucket.getAggregations().get(agg2Name);
        Long inodeId = (Long)bucket.getKeyAsNumber();
        Long lastAccessed = ((Number) agg2.getValue()).longValue();
        result.add(new ProvFileOpDTO.FileAggregation(inodeId, bucket.getDocCount(), lastAccessed));
      }
      return result;
    };
  }
  
  private static ElasticAggregationParser<ProvFileOpDTO.Artifact, ProvenanceException> appArtifactFootprintAParser() {
    return (Aggregations aggregations) -> {
      List<ProvFileOpDTO.Artifact> result = new LinkedList<>();
      Terms artifacts = aggregations.get("artifacts");
      if(artifacts == null) {
        return result;
      }
      for(Terms.Bucket artifactBucket : artifacts.getBuckets()) {
        ProvFileOpDTO.Artifact artifact = new ProvFileOpDTO.Artifact();
  
        Terms files = artifactBucket.getAggregations().get("files");
        if(files == null) {
          continue;
        }
        result.add(artifact);
        
        ProvFileOpDTO.ArtifactBase base = null;
        for(Terms.Bucket fileBucket : artifacts.getBuckets()) {
          ProvFileOpDTO.ArtifactFile file = new ProvFileOpDTO.ArtifactFile();
          //create
          Filter createFilter = fileBucket.getAggregations().get("create");
          if(createFilter != null) {
            TopHits createOpHits = createFilter.getAggregations().get("create_op");
            if(createOpHits.getHits().getTotalHits() > 1) {
              throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.INTERNAL_ERROR, Level.WARNING,
                "cannot have two create ops on the same inode");
            }
            BasicElasticHit hit = BasicElasticHit.instance(createOpHits.getHits().getAt(0));
            ProvFileOpElastic createOp = ProvFileOpElastic.instance(hit);
            file.addCreate(createOp);
            base = extractBaseIfNotExists(base, createOp);
          }
  
          //delete
          Filter deleteFilter = fileBucket.getAggregations().get("delete");
          if(deleteFilter != null) {
            TopHits deleteOpHits = deleteFilter.getAggregations().get("delete_op");
            if(deleteOpHits.getHits().getTotalHits() > 1) {
              throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.INTERNAL_ERROR, Level.WARNING,
                "cannot have two delete ops on the same inode");
            }
            BasicElasticHit hit = BasicElasticHit.instance(deleteOpHits.getHits().getAt(0));
            ProvFileOpElastic deleteOp = ProvFileOpElastic.instance(hit);
            file.addDelete(deleteOp);
            base = extractBaseIfNotExists(base, deleteOp);
          }
  
          //read
          Filter readFilter = fileBucket.getAggregations().get("read");
          if(readFilter != null) {
            TopHits readOpHits = readFilter.getAggregations().get("first_read");
            BasicElasticHit hit = BasicElasticHit.instance(readOpHits.getHits().getAt(0));
            ProvFileOpElastic readOp = ProvFileOpElastic.instance(hit);
            file.addFirstRead(readOp, readOpHits.getHits().getTotalHits());
            base = extractBaseIfNotExists(base, readOp);
          }
  
          //append
          Filter appendFilter = fileBucket.getAggregations().get("append");
          if(appendFilter != null) {
            TopHits appendOpHits = appendFilter.getAggregations().get("first_append");
            BasicElasticHit hit = BasicElasticHit.instance(appendOpHits.getHits().getAt(0));
            ProvFileOpElastic appendOp = ProvFileOpElastic.instance(hit);
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
  
  private static ProvFileOpDTO.ArtifactBase extractBaseIfNotExists(ProvFileOpDTO.ArtifactBase base,
    ProvFileOpElastic op) {
    if(base != null) {
      return base;
    }
  
    ProvParser.DocSubType mlType = ProvParser.DocSubType.valueOf(op.getDocSubType().name());
    return new ProvFileOpDTO.ArtifactBase(op.getProjectInodeId(), op.getDatasetInodeId(), op.getMlId(), mlType);
  }
  
  private static AggregationBuilder appArtifactFootprintABuilder() {
    return
      AggregationBuilders.terms("artifacts")
        .field(ProvParser.BaseField.ML_ID.toString())
        .subAggregation(
          AggregationBuilders.terms("files")
            .field(ProvParser.BaseField.INODE_ID.toString())
            .subAggregation(AggregationBuilders
              .filter("create", termQuery(ProvOParser.BaseField.INODE_OPERATION.toString(),
                  Provenance.FileOps.CREATE.toString()))
              .subAggregation(AggregationBuilders
                .topHits("create_op")
                .sort(ProvOParser.BaseField.TIMESTAMP.toString(), SortOrder.ASC)
                .size(1)))
            .subAggregation(AggregationBuilders
              .filter("delete", termQuery(ProvOParser.BaseField.INODE_OPERATION.toString(),
                Provenance.FileOps.ACCESS_DATA.toString()))
              .subAggregation(AggregationBuilders
                .topHits("delete_op")
                .sort(ProvOParser.BaseField.TIMESTAMP.toString(), SortOrder.ASC)
                .size(1)))
            .subAggregation(AggregationBuilders
              .filter("read", termQuery(ProvOParser.BaseField.INODE_OPERATION.toString(),
                Provenance.FileOps.ACCESS_DATA.toString()))
              .subAggregation(AggregationBuilders
                .topHits("first_read")
                .sort(ProvOParser.BaseField.TIMESTAMP.toString(), SortOrder.ASC)
                .size(1)))
            .subAggregation(AggregationBuilders
              .filter("append", termQuery(ProvOParser.BaseField.INODE_OPERATION.toString(),
                Provenance.FileOps.ACCESS_DATA.toString()))
              .subAggregation(AggregationBuilders
                .topHits("first_append")
                .sort(ProvOParser.BaseField.TIMESTAMP.toString(), SortOrder.ASC)
                .size(1))));
  }
}
