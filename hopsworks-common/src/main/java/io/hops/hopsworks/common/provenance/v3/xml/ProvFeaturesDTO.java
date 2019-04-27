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
package io.hops.hopsworks.common.provenance.v3.xml;

import io.hops.hopsworks.common.dao.featurestore.feature.FeatureDTO;
import io.hops.hopsworks.common.provenance.v2.ProvXAttrs;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlList;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.LinkedList;
import java.util.List;

@XmlRootElement
public class ProvFeaturesDTO {
  @XmlList
  @XmlElement(name= ProvXAttrs.PROV_XATTR_FEATURES)
  private List<ProvFeatureDTO> features = new LinkedList<>();
  
  public ProvFeaturesDTO() {}
  
  public ProvFeaturesDTO(List<ProvFeatureDTO> features) {
    this.features = features;
  }
  
  public List<ProvFeatureDTO> getFeatures() {
    return features;
  }
  
  public void setFeatures(List<ProvFeatureDTO> features) {
    this.features = features;
  }
  
  public static ProvFeaturesDTO fromFeatures(List<FeatureDTO> features) {
    List<ProvFeatureDTO> aux = new LinkedList<>();
    for(FeatureDTO feature : features) {
      aux.add(new ProvFeatureDTO(feature.getFeaturegroup(), feature.getName(), feature.getVersion()));
    }
    return new ProvFeaturesDTO(aux);
  }
}
