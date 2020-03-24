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

import io.hops.hopsworks.common.dao.dataset.DatasetFacade;
import io.hops.hopsworks.common.dao.dataset.DatasetSharedWithFacade;
import io.hops.hopsworks.common.dao.hdfs.inode.InodeFacade;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.dao.project.team.ProjectTeamFacade;
import io.hops.hopsworks.common.elastic.FeaturestoreDocType;
import io.hops.hopsworks.common.hdfs.Utils;
import io.hops.hopsworks.persistence.entity.dataset.Dataset;
import io.hops.hopsworks.persistence.entity.dataset.DatasetSharedWith;
import io.hops.hopsworks.persistence.entity.hdfs.inode.Inode;
import io.hops.hopsworks.persistence.entity.project.Project;
import io.hops.hopsworks.persistence.entity.user.Users;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class DataAccessController {
  private static final Logger LOGGER = Logger.getLogger(DataAccessController.class.getName());
  
  @EJB
  private ProjectFacade projectFacade;
  @EJB
  private ProjectTeamFacade projectTeamFacade;
  @EJB
  private InodeFacade inodeFacade;
  @EJB
  private DatasetFacade datasetFacade;
  @EJB
  private DatasetSharedWithFacade dsSharedWithFacade;
  
  public ShortLivedCache newCache() {
    return new ShortLivedCache();
  }
  
  public Map<FeaturestoreDocType, Set<Integer>> featurestoreSearchContext(Project targetProject,
    FeaturestoreDocType docType) {
    Map<FeaturestoreDocType, Set<Integer>> searchProjects = new HashMap<>();
    Set<Integer> featuregroupProjects = new HashSet<>();
    Set<Integer> trainingdatasetProjects = new HashSet<>();
    switch(docType) {
      case FEATURE:
      case FEATUREGROUP: {
        featuregroupProjects.add(targetProject.getId());
        searchProjects.put(FeaturestoreDocType.FEATUREGROUP, featuregroupProjects);
      }
        break;
      case TRAININGDATASET: {
        trainingdatasetProjects.add(targetProject.getId());
        searchProjects.put(FeaturestoreDocType.TRAININGDATASET, trainingdatasetProjects);
      }
        break;
      case ALL: {
        featuregroupProjects.add(targetProject.getId());
        searchProjects.put(FeaturestoreDocType.FEATUREGROUP, featuregroupProjects);
        trainingdatasetProjects.add(targetProject.getId());
        searchProjects.put(FeaturestoreDocType.TRAININGDATASET, trainingdatasetProjects);
      }
        break;
    }
    List<DatasetSharedWith> sharedDS = dsSharedWithFacade.findByProject(targetProject);
    for(DatasetSharedWith ds : sharedDS) {
      if(Utils.getFeaturestoreName(ds.getProject()).equals(ds.getDataset().getName())) {
        featuregroupProjects.add(ds.getProject().getId());
      }
      if(Utils.getTrainingDatasetName(ds.getProject()).equals(ds.getDataset().getName())) {
        trainingdatasetProjects.add(ds.getProject().getId());
      }
    }
    return searchProjects;
  }
  
  public <E> void addAccessProjects(Users user, AccessI interfaceMapper, ShortLivedCache cache) {
    //<project, userProjectRole>
    Pair<Project, String> projectAux = getProjectWithCache(user, interfaceMapper.getParentProjectId(), cache);
    //check if parent project and parent dataset still exist or is this a stale item
    if(projectAux == null) {
      LOGGER.log(Level.FINE, "parent project of item - not found - probably stale item:{0}", interfaceMapper);
      return;
    }
    Dataset dataset = getDatasetWithCache(projectAux.getValue0(), interfaceMapper.getParentDatasetIId(), cache);
    if(dataset == null) {
      LOGGER.log(Level.FINE, "parent dataset of item - not found - probably stale item:{0}", interfaceMapper);
      return;
    }
    //check parent project for access
    if (projectAux.getValue1() != null) {
      interfaceMapper.addAccessProject(projectAux.getValue0());
    }
    //check shared datasets for access
    checkSharedDatasetsAccess(user, dataset, interfaceMapper, cache);
  }
  
  private void checkSharedDatasetsAccess(Users user, Dataset dataset, AccessI interfaceMapper, ShortLivedCache cache) {
    if(cache.sharedProjectsCache.containsKey(dataset.getInodeId())) {
      //cached
      Set<Integer> projectIds  = cache.sharedProjectsCache.get(dataset.getInodeId());
      for(Integer projectId : projectIds) {
        Pair<Project, String> projectAux = getProjectWithCache(user, projectId, cache);
        if(projectAux != null && projectAux.getValue1() != null) {
          interfaceMapper.addAccessProject(projectAux.getValue0());
        }
      }
    } else {
      //not yet cached
      List<DatasetSharedWith> dsSharedWith = dsSharedWithFacade.findByDataset(dataset);
      Set<Integer> projectIds = new HashSet<>();
      cache.sharedProjectsCache.put(dataset.getInodeId(), projectIds);
      for(DatasetSharedWith ds : dsSharedWith) {
        projectIds.add(ds.getProject().getId());
        Pair<Project, String> projectAux = getProjectWithCache(user, ds.getProject(), cache);
        if(projectAux != null && projectAux.getValue1() != null) {
          interfaceMapper.addAccessProject(projectAux.getValue0());
        }
      }
    }
  }
  
  private Dataset getDatasetWithCache(Project project, Long datasetIId, ShortLivedCache cache) {
    if(cache.datasetCache.containsKey(datasetIId)) {
      return cache.datasetCache.get(datasetIId);
    } else {
      Inode datasetInode = inodeFacade.findById(datasetIId);
      if(datasetInode == null) {
        return null;
      }
      Dataset dataset = datasetFacade.findByProjectAndInode(project, datasetInode);
      if(dataset == null) {
        return null;
      }
      cache.datasetCache.put(dataset.getInodeId(), dataset);
      return dataset;
    }
  }
  
  private Pair<Project, String> getProjectWithCache(Users user, Integer projectId, ShortLivedCache cache) {
    if (cache.projectCache.containsKey(projectId)) {
      return cache.projectCache.get(projectId);
    } else {
      Project project = projectFacade.find(projectId);
      if(project == null) {
        return null;
      }
      String projectRole = projectTeamFacade.findCurrentRole(project, user);
      cache.projectCache.put(project.getId(), Pair.with(project, projectRole));
      return Pair.with(project, projectRole);
    }
  }
  
  private Pair<Project, String> getProjectWithCache(Users user, Project project, ShortLivedCache cache) {
    if (cache.projectCache.containsKey(project.getId())) {
      return cache.projectCache.get(project.getId());
    } else {
      String projectRole = projectTeamFacade.findCurrentRole(project, user);
      cache.projectCache.put(project.getId(), Pair.with(project, projectRole));
      return Pair.with(project, projectRole);
    }
  }
  
  public interface AccessI {
    Integer getParentProjectId();
    Long getParentDatasetIId();
    void addAccessProject(Project project);
    String toString();
  }
  
  public static class ShortLivedCache {
    Map<Integer, Pair<Project, String>> projectCache = new HashMap<>();
    Map<Long, Dataset> datasetCache = new HashMap<>();
    Map<Long, Set<Integer>> sharedProjectsCache = new HashMap<>();
  }
}
