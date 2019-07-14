/*
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
 */

package io.hops.hopsworks.common.dao.featurestore.feature;

import com.google.gson.Gson;
import io.hops.hopsworks.common.dao.featurestore.featuregroup.on_demand_featuregroup.OnDemandFeaturegroup;
import io.hops.hopsworks.common.dao.featurestore.trainingdataset.TrainingDataset;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.apache.hadoop.fs.XAttrSetFlag;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.io.IOException;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Class controlling the interaction with the training_dataset_feature table and required business logic
 */
@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class FeaturestoreFeatureController {
  @EJB
  private FeaturestoreFeatureFacade featurestoreFeatureFacade;
  @EJB
  private DistributedFsService dfs;

  /**
   * Updates the features of a training dataset, first deletes all existing features for the training dataset
   * and then insert the new ones.
   *
   * @param trainingDataset the training dataset to update
   * @param features the new features
   */
  public void updateTrainingDatasetFeatures(TrainingDataset trainingDataset, List<FeatureDTO> features,
    Optional<String> trainingDatasetHopsPath) throws GenericException {
    if(features == null) {
      return;
    }
    removeFeatures((List) trainingDataset.getFeatures());
    insertTrainingDatasetFeatures(trainingDataset, features);
    if(trainingDatasetHopsPath.isPresent()) {
      setXAttrTrainingFeatures(trainingDatasetHopsPath.get(), features);
    }
  }

  /**
   * Removes a list of features from the database
   *
   * @param featurestoreFeatures list of features to remove
   */
  private void removeFeatures(List<FeaturestoreFeature> featurestoreFeatures) {
    featurestoreFeatureFacade.deleteListOfFeatures(featurestoreFeatures.stream().map(
      f -> f.getId()).collect(Collectors.toList()));
  }

  /**
   * Inserts a list of features into the database
   *
   * @param trainingDataset the traning dataset that the features are linked to
   * @param features the list of features to insert
   */
  private void insertTrainingDatasetFeatures(
      TrainingDataset trainingDataset, List<FeatureDTO> features) {
    List<FeaturestoreFeature> featurestoreFeatures = convertFeaturesToTrainingDatasetFeatures(
        trainingDataset, features);
    featurestoreFeatureFacade.persist(featurestoreFeatures);
  }

  /**
   * Utility method that converts a list of featureDTOs to FeaturestoreFeature entities
   *
   * @param trainingDataset the training dataset that the features are linked to
   * @param features the list of feature DTOs to convert
   * @return a list of FeaturestoreFeature entities
   */
  private List<FeaturestoreFeature> convertFeaturesToTrainingDatasetFeatures(
      TrainingDataset trainingDataset, List<FeatureDTO> features) {
    return features.stream().map(f -> {
      FeaturestoreFeature featurestoreFeature = new FeaturestoreFeature();
      featurestoreFeature.setName(f.getName());
      featurestoreFeature.setTrainingDataset(trainingDataset);
      featurestoreFeature.setDescription(f.getDescription());
      featurestoreFeature.setPrimary(f.getPrimary()? 1 : 0);
      featurestoreFeature.setType(f.getType());
      return featurestoreFeature;
    }).collect(Collectors.toList());
  }
  
  
  /**
   * Updates the features of an on-demand Feature Group, first deletes all existing features for the on-demand Feature
   * Group and then insert the new ones.
   *
   * @param onDemandFeaturegroup the on-demand featuregroup to update
   * @param features the new features
   */
  public void updateOnDemandFeaturegroupFeatures(
      OnDemandFeaturegroup onDemandFeaturegroup, List<FeatureDTO> features) {
    if(features == null) {
      return;
    }
    removeFeatures((List) onDemandFeaturegroup.getFeatures());
    insertOnDemandFeaturegroupFeatures(onDemandFeaturegroup, features);
  }
  
  /**
   * Inserts a list of features into the database
   *
   * @param onDemandFeaturegroup the on-demand feature group that the features are linked to
   * @param features the list of features to insert
   */
  private void insertOnDemandFeaturegroupFeatures(
    OnDemandFeaturegroup onDemandFeaturegroup, List<FeatureDTO> features) {
    List<FeaturestoreFeature> featurestoreFeatures = convertFeaturesToOnDemandFeaturegroupFeatures(
      onDemandFeaturegroup, features);
    featurestoreFeatureFacade.persist(featurestoreFeatures);
  }
  
  /**
   * Utility method that converts a list of featureDTOs to FeaturestoreFeature entities
   *
   * @param onDemandFeaturegroup the on-demand featuregroup that the features are linked to
   * @param features the list of feature DTOs to convert
   * @return a list of FeaturestoreFeature entities
   */
  private List<FeaturestoreFeature> convertFeaturesToOnDemandFeaturegroupFeatures(
    OnDemandFeaturegroup onDemandFeaturegroup, List<FeatureDTO> features) {
    return features.stream().map(f -> {
      FeaturestoreFeature featurestoreFeature = new FeaturestoreFeature();
      featurestoreFeature.setName(f.getName());
      featurestoreFeature.setOnDemandFeaturegroup(onDemandFeaturegroup);
      featurestoreFeature.setDescription(f.getDescription());
      featurestoreFeature.setPrimary(f.getPrimary()? 1 : 0);
      featurestoreFeature.setType(f.getType());
      return featurestoreFeature;
    }).collect(Collectors.toList());
  }
  
  /**
   * Attach features as xattrs to the training dataset dir
   */
  private void setXAttrTrainingFeatures(String tdPath, List<FeatureDTO> features) throws GenericException {
    Gson gson = new Gson();
    XAttrFeatures jsonFeatures = new XAttrFeatures();
    for(FeatureDTO feature : features) {
      jsonFeatures.addFeature(feature);
    }
  
    byte[] featuresValue = gson.toJson(jsonFeatures.getFeatures()).getBytes();
    DistributedFileSystemOps dfso = dfs.getDfsOps();
    EnumSet<XAttrSetFlag> flags = EnumSet.noneOf(XAttrSetFlag.class);
    flags.add(XAttrSetFlag.CREATE);
    try {
      dfso.setXAttr(tdPath, "provenance.features", featuresValue, flags);
    } catch (IOException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "xattrs persistance exception");
    }
  }
  
  private static class XAttrFeatures {
    private List<XAttrFeature> features = new LinkedList<>();
    
    public XAttrFeatures() {}
    
    public XAttrFeatures(List<XAttrFeature> features) {
      this.features = features;
    }
    
    public List<XAttrFeature> getFeatures() {
      return features;
    }
    
    public void setFeatures(List<XAttrFeature> features) {
      this.features = features;
    }
    
    public void addFeature(FeatureDTO feature) {
      features.add(new XAttrFeature("group", feature.getName(), "version"));
    }
  }
  
  private static class XAttrFeature {
    private String group;
    private String name;
    private String version;
    
    public XAttrFeature() {}
    
    public XAttrFeature(String group, String name, String version) {
      this.group = group;
      this.name = name;
      this.version = version;
    }
    
    public String getGroup() {
      return group;
    }
    
    public void setGroup(String group) {
      this.group = group;
    }
    
    public String getName() {
      return name;
    }
    
    public void setName(String name) {
      this.name = name;
    }
    
    public String getVersion() {
      return version;
    }
    
    public void setVersion(String version) {
      this.version = version;
    }
  }
}
