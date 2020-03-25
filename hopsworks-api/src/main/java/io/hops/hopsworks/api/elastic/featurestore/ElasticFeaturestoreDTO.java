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
  private List<ElasticFeaturestoreItemDTO> featuregroups = new LinkedList<>();
  private List<ElasticFeaturestoreItemDTO> trainingdatasets = new LinkedList<>();
  
  public ElasticFeaturestoreDTO() {
  }
  
  public ElasticFeaturestoreDTO(List<ElasticFeaturestoreItemDTO> featuregroups,
    List<ElasticFeaturestoreItemDTO> trainingdatasets) {
    this.featuregroups = featuregroups;
    this.trainingdatasets = trainingdatasets;
  }
  
  public List<ElasticFeaturestoreItemDTO> getFeaturegroups() {
    return featuregroups;
  }
  
  public void setFeaturegroups(List<ElasticFeaturestoreItemDTO> featuregroups) {
    this.featuregroups = featuregroups;
  }
  
  public List<ElasticFeaturestoreItemDTO> getTrainingdatasets() {
    return trainingdatasets;
  }
  
  public void setTrainingdatasets(List<ElasticFeaturestoreItemDTO> trainingdatasets) {
    this.trainingdatasets = trainingdatasets;
  }
  
  public void addTrainingdataset(ElasticFeaturestoreItemDTO trainingdataset) {
    trainingdatasets.add(trainingdataset);
  }
  
  public void addFeaturegroup(ElasticFeaturestoreItemDTO featuregroup) {
    featuregroups.add(featuregroup);
  }
}
