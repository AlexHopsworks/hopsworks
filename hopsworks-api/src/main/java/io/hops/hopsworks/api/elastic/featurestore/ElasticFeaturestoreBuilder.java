/*
 * This file is part of Hopsworks
 * Copyright (C) 2020, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.api.elastic.featurestore;

import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.elastic.ElasticController;
import io.hops.hopsworks.common.elastic.FeaturestoreDocType;
import io.hops.hopsworks.common.elastic.FeaturestoreElasticHit;
import io.hops.hopsworks.common.provenance.core.ProvXAttrs;
import io.hops.hopsworks.common.util.HopsworksJAXBContext;
import io.hops.hopsworks.exceptions.ElasticException;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.persistence.entity.project.Project;
import io.hops.hopsworks.persistence.entity.user.Users;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.javatuples.Pair;


import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static io.hops.hopsworks.common.provenance.core.ProvXAttrs.Featurestore;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ElasticFeaturestoreBuilder {
  @EJB
  private ElasticController elasticCtrl;
  @Inject
  private DataAccessController dataAccessCtrl;
  @EJB
  private ProjectFacade projectFacade;
  @EJB
  private HopsworksJAXBContext converter;
  
  public ElasticFeaturestoreDTO build(ElasticFeaturestoreRequest req, Integer projectId)
    throws ElasticException, ServiceException, GenericException {
    Project project = projectFacade.find(projectId);
    Map<FeaturestoreDocType, Set<Integer>> searchProjects
      = dataAccessCtrl.featurestoreSearchContext(project, req.getDocType());
    Pair<SearchHit[], SearchHit[]> hits = elasticCtrl.featurestoreSearch(req.getTerm(), searchProjects);
    return parseResult(req.getDocType(), hits.getValue1(), accessFromParentProject(project));
  }
  
  public ElasticFeaturestoreDTO build(Users user,ElasticFeaturestoreRequest req)
    throws ElasticException, ServiceException, GenericException {
    Pair<SearchHit[], SearchHit[]> hits = elasticCtrl.featurestoreSearch(req.getDocType(), req.getTerm());
    return parseResult(req.getDocType(), hits.getValue1(), accessFromSharedProjects(user));
  }
  
  private ElasticFeaturestoreDTO parseResult(FeaturestoreDocType docType, SearchHit[] hits,
    ProjectAccessCtrl accessCtrl)
    throws ElasticException, GenericException {
    ElasticFeaturestoreDTO result = new ElasticFeaturestoreDTO();
    for(SearchHit hitAux : hits) {
      FeaturestoreElasticHit hit = FeaturestoreElasticHit.instance(hitAux);
      if(FeaturestoreDocType.TRAININGDATASET.toString().toLowerCase().equals(hit.getDocType())
        && (FeaturestoreDocType.ALL.equals(docType) || FeaturestoreDocType.TRAININGDATASET.equals(docType))) {
        ElasticFeaturestoreItemDTO.Base item = ElasticFeaturestoreItemDTO.fromTrainingDataset(hit, converter);
        result.addTrainingdataset(item);
        accessCtrl.accept(item, hit);
        item.setHighlights(getHighlights(hitAux.getHighlightFields()));
      }
      if(FeaturestoreDocType.FEATUREGROUP.toString().toLowerCase().equals(hit.getDocType())
        && (FeaturestoreDocType.ALL.equals(docType) || FeaturestoreDocType.FEATUREGROUP.equals(docType))) {
        ElasticFeaturestoreItemDTO.Base item = ElasticFeaturestoreItemDTO.fromFeaturegroup(hit, converter);
        result.addFeaturegroup(item);
        accessCtrl.accept(item, hit);
        item.setHighlights(getHighlights(hitAux.getHighlightFields()));
      }
      if(FeaturestoreDocType.FEATUREGROUP.toString().toLowerCase().equals(hit.getDocType())
        && (FeaturestoreDocType.ALL.equals(docType) || FeaturestoreDocType.FEATURE.equals(docType))) {
        ElasticFeaturestoreItemDTO.Base fgParent = ElasticFeaturestoreItemDTO.fromFeaturegroup(hit, converter);
        Map<String, HighlightField> highlightFields = hitAux.getHighlightFields();
        String featureField = Featurestore.getFeaturestoreElasticKey(Featurestore.FG_FEATURES);
        if (highlightFields.containsKey(featureField)) {
          for (Text e : highlightFields.get(featureField).fragments()) {
            String feature = removeHighlightTags(e.toString());
            ElasticFeaturestoreItemDTO.Feature item = ElasticFeaturestoreItemDTO.fromFeature(feature, fgParent);
            result.addFeature(item);
            accessCtrl.accept(item, hit);
            ElasticFeaturestoreItemDTO.Highlights highlights = new ElasticFeaturestoreItemDTO.Highlights();
            highlights.setName(true);
            item.setHighlights(highlights);
          }
        }
      }
    }
    return result;
  }
  
  private ElasticFeaturestoreItemDTO.Highlights getHighlights(Map<String, HighlightField> map) {
    ElasticFeaturestoreItemDTO.Highlights highlights = new ElasticFeaturestoreItemDTO.Highlights();
    for(String key : map.keySet()) {
      if(key.equals(Featurestore.NAME)
        || key.equals(Featurestore.getFeaturestoreElasticKey(Featurestore.NAME))
        || key.equals(Featurestore.getFeaturestoreElasticKey(Featurestore.NAME) + ".keyword")) {
        highlights.setName(true);
        continue;
      }
      if(key.equals(Featurestore.getFeaturestoreElasticKey(Featurestore.DESCRIPTION))
        || key.equals(Featurestore.getFeaturestoreElasticKey(Featurestore.DESCRIPTION) + ".keyword")) {
        highlights.setDescription(true);
        continue;
      }
      if(key.equals(Featurestore.getFeaturestoreElasticKey(Featurestore.FG_FEATURES))
        || key.equals(Featurestore.getFeaturestoreElasticKey(Featurestore.FG_FEATURES) + ".keyword")) {
        highlights.setFeature(true);
        continue;
      }
      if(key.equals(Featurestore.getFeaturestoreElasticKey(Featurestore.TD_FEATURES))
        || key.equals(Featurestore.getFeaturestoreElasticKey(Featurestore.TD_FEATURES) + ".keyword")) {
        highlights.setFeature(true);
        continue;
      }
      if(key.equals(Featurestore.getFeaturestoreElasticKey(Featurestore.TD_FEATURES))
        || key.equals(Featurestore.getFeaturestoreElasticKey(Featurestore.TD_FEATURES) + ".keyword")) {
        highlights.setFeature(true);
        continue;
      }
      if(key.startsWith(ProvXAttrs.ELASTIC_XATTR + ".")) {
        highlights.setOtherXattr(true);
      }
    }
    return highlights;
  }
  
  private String removeHighlightTags(String field) {
    field = field.replace("<em>", "");
    field = field.replace("</em>", "");
    return field;
  }
  
  private ProjectAccessCtrl accessFromParentProject(Project project) {
    return (item, elasticHit) -> item.addAccessProject(project.getId(), project.getName());
  }
  
  private ProjectAccessCtrl accessFromSharedProjects(Users user) {
    DataAccessController.ShortLivedCache cache = dataAccessCtrl.newCache();
    return (item, elasticHit) -> dataAccessCtrl.addAccessProjects(user, accessMapper(item, elasticHit), cache);
  }
  
  private interface ProjectAccessCtrl extends BiConsumer<ElasticFeaturestoreItemDTO.Base, FeaturestoreElasticHit> {
  }
  
  private DataAccessController.AccessI accessMapper(ElasticFeaturestoreItemDTO.Base item, FeaturestoreElasticHit hit) {
    return new DataAccessController.AccessI() {
  
      @Override
      public Integer getParentProjectId() {
        return hit.getProjectId();
      }
  
      @Override
      public Long getParentDatasetIId() {
        return hit.getDatasetIId();
      }
  
      @Override
      public void addAccessProject(Project project) {
        item.addAccessProject(project.getId(), project.getName());
      }
  
      @Override
      public String toString() {
        return hit.toString();
      }
    };
  }
}
