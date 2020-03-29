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
import io.hops.hopsworks.common.util.HopsworksJAXBContext;
import io.hops.hopsworks.exceptions.ElasticException;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.persistence.entity.project.Project;
import io.hops.hopsworks.persistence.entity.user.Users;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  
  public ElasticFeaturestoreDTO build(Users user, ElasticFeaturestoreRequest req)
    throws ElasticException, ServiceException, GenericException {
    ElasticFeaturestoreDTO result = new ElasticFeaturestoreDTO();
    Pair<List<FeaturestoreElasticHit>, List<FeaturestoreElasticHit>> hits =
      elasticCtrl.featurestoreSearch(req.getDocType(), req.getTerm());
    DataAccessController.ShortLivedCache cache = dataAccessCtrl.newCache();
    for(FeaturestoreElasticHit hit : hits.getValue1()) {
      ElasticFeaturestoreItemDTO item = null;
      if(FeaturestoreDocType.TRAININGDATASET.toString().toLowerCase().equals(hit.getDocType())) {
        item = ElasticFeaturestoreItemDTO.fromTrainingDataset(hit, converter);
        result.addTrainingdataset(item);
      } else if(FeaturestoreDocType.FEATUREGROUP.toString().toLowerCase().equals(hit.getDocType())) {
        item = ElasticFeaturestoreItemDTO.fromFeaturegroup(hit, converter);
        result.addTrainingdataset(item);
      }
      dataAccessCtrl.addAccessProjects(user, accessMapper(item, hit), cache);
    }
    return result;
  }
  
  private DataAccessController.AccessI accessMapper(ElasticFeaturestoreItemDTO item, FeaturestoreElasticHit hit) {
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
  
  public ElasticFeaturestoreDTO build(ElasticFeaturestoreRequest req, Integer projectId)
    throws ElasticException, ServiceException, GenericException {
    ElasticFeaturestoreDTO result = new ElasticFeaturestoreDTO();
    
    Project project = projectFacade.find(projectId);
    //search context
    Map<FeaturestoreDocType, Set<Integer>> searchProjects
      = dataAccessCtrl.featurestoreSearchContext(project, req.getDocType());
    
    Pair<List<FeaturestoreElasticHit>,List<FeaturestoreElasticHit>> hits =
      elasticCtrl.featurestoreSearch(req.getTerm(), searchProjects);
  
    for(FeaturestoreElasticHit hit : hits.getValue1()) {
      ElasticFeaturestoreItemDTO item = null;
      if(FeaturestoreDocType.TRAININGDATASET.toString().toLowerCase().equals(hit.getDocType())) {
        item = ElasticFeaturestoreItemDTO.fromTrainingDataset(hit, converter);
        result.addTrainingdataset(item);
      } else if(FeaturestoreDocType.FEATUREGROUP.toString().toLowerCase().equals(hit.getDocType())) {
        item = ElasticFeaturestoreItemDTO.fromFeaturegroup(hit, converter);
        result.addFeaturegroup(item);
      }
      if(item != null) {
        item.addAccessProject(hit.getProjectId(), hit.getProjectName());
      }
    }
    return result;
  }
}
