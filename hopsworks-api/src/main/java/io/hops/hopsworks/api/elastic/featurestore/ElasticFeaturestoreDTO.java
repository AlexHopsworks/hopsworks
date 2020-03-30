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

import javax.xml.bind.annotation.XmlRootElement;
import java.util.LinkedList;
import java.util.List;

@XmlRootElement
public class ElasticFeaturestoreDTO {
  private List<ElasticFeaturestoreItemDTO.Base> featuregroups = new LinkedList<>();
  private List<ElasticFeaturestoreItemDTO.Base> trainingdatasets = new LinkedList<>();
  private List<ElasticFeaturestoreItemDTO.Feature> features = new LinkedList<>();
  
  public ElasticFeaturestoreDTO() {
  }
  
  public ElasticFeaturestoreDTO(List<ElasticFeaturestoreItemDTO.Base> featuregroups,
    List<ElasticFeaturestoreItemDTO.Base> trainingdatasets, List<ElasticFeaturestoreItemDTO.Feature> features) {
    this.featuregroups = featuregroups;
    this.trainingdatasets = trainingdatasets;
    this.features = features;
  }
  
  public List<ElasticFeaturestoreItemDTO.Base> getFeaturegroups() {
    return featuregroups;
  }
  
  public void setFeaturegroups(List<ElasticFeaturestoreItemDTO.Base> featuregroups) {
    this.featuregroups = featuregroups;
  }
  
  public List<ElasticFeaturestoreItemDTO.Base> getTrainingdatasets() {
    return trainingdatasets;
  }
  
  public void setTrainingdatasets(List<ElasticFeaturestoreItemDTO.Base> trainingdatasets) {
    this.trainingdatasets = trainingdatasets;
  }
  
  public List<ElasticFeaturestoreItemDTO.Feature> getFeatures() {
    return features;
  }
  
  public void setFeatures(
    List<ElasticFeaturestoreItemDTO.Feature> features) {
    this.features = features;
  }
  
  public void addTrainingdataset(ElasticFeaturestoreItemDTO.Base trainingdataset) {
    trainingdatasets.add(trainingdataset);
  }
  
  public void addFeaturegroup(ElasticFeaturestoreItemDTO.Base featuregroup) {
    featuregroups.add(featuregroup);
  }
  
  public void addFeature(ElasticFeaturestoreItemDTO.Feature feature) {
    features.add(feature);
  }
}
