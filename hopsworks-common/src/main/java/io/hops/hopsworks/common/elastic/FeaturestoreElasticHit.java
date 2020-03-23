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

import io.hops.hopsworks.exceptions.ElasticException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.search.SearchHit;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a JSONifiable version of the featurestore elastic hit object
 */
@XmlRootElement
public class FeaturestoreElasticHit implements Comparator<FeaturestoreElasticHit> {
  
  private static final Logger LOG = Logger.getLogger(FeaturestoreElasticHit.class.getName());
  
  //the inode id
  private String id;
  private float score;
  private Map<String, Object> map;
  
  //inode name
  private String docType;
  private String name;
  private Integer version;
  private Integer projectId;
  private String projectName;
  private Set<String> features = new HashSet<>();
  private Map<String, String> tags = new HashMap<>();
  private Map<String, String> otherXAttrs = new HashMap<>();

  public FeaturestoreElasticHit() {
  }
  
  public FeaturestoreElasticHit(String id, float score, Map<String, Object> map,
    String docType, String name, int version, int projectId, String projectName,
    Set<String> features,  Map<String, String> tags, Map<String, String> otherXAttrs) {
    this.id = id;
    this.score = score;
    this.map = map;
    
    this.docType = docType;
    this.name = name;
    this.version = version;
    this.projectId = projectId;
    this.projectName = projectName;
    this.features = features;
    this.tags = tags;
    this.otherXAttrs = otherXAttrs;
    
  }
  
  public static FeaturestoreElasticHit instance(SearchHit hit) throws ElasticException {
    FeaturestoreElasticHit feHit = new FeaturestoreElasticHit();
    feHit.id = hit.getId();
    //the source of the retrieved record (i.e. all the indexed information)
    feHit.map = hit.getSourceAsMap();
    feHit.score = hit.getScore();
  
    try {
      for (Map.Entry<String, Object> entry : feHit.map.entrySet()) {
        //set the name explicitly so that it's easily accessible in the frontend
        if (entry.getKey().equals("doc_type")) {
          feHit.docType = entry.getValue().toString();
        } else if (entry.getKey().equals("name")) {
          feHit.name = entry.getValue().toString();
        } else if (entry.getKey().equals("version")) {
          feHit.version = Integer.parseInt(entry.getValue().toString());
        } else if (entry.getKey().equals("project_id")) {
          feHit.projectId = Integer.parseInt(entry.getValue().toString());
        } else if (entry.getKey().equals("project_name")) {
          feHit.projectName = entry.getValue().toString();
        } else if (entry.getKey().equals("xattr")) {
          Map<String, Object> xattrs = (Map)entry.getValue();
          for(Map.Entry<String, Object> e : xattrs.entrySet()) {
            if(e.getKey().equals("features")) {
              feHit.features.add(e.getValue().toString());
            } else if(e.getKey().equals("tags")) {
              Map<String, Object> tags = (Map)e.getValue();
              for(Map.Entry<String, Object> ee : tags.entrySet()) {
                feHit.tags.put(ee.getKey(), ee.getValue().toString());
              }
            } else {
              feHit.otherXAttrs.put(e.getKey(), e.getValue().toString());
            }
          }
        }
      }
    } catch (NumberFormatException e) {
      throw new ElasticException(RESTCodes.ElasticErrorCode.ELASTIC_INTERNAL_REQ_ERROR,
        Level.WARNING, "Hopsworks and Elastic types do not match - number problem");
    }
    return feHit;
  }
  
  @Override
  public int compare(FeaturestoreElasticHit o1, FeaturestoreElasticHit o2) {
    return Float.compare(o2.getScore(), o1.getScore());
  }
  
  public String getId() {
    return id;
  }
  
  public void setId(String id) {
    this.id = id;
  }
  
  public float getScore() {
    return score;
  }
  
  public void setScore(float score) {
    this.score = score;
  }
  
  public Map<String, String> getMap() {
    //flatten hits (remove nested json objects) to make it more readable
    Map<String, String> refined = new HashMap<>();
    
    if (this.map != null) {
      for (Map.Entry<String, Object> entry : this.map.entrySet()) {
        //convert value to string
        String value = (entry.getValue() == null) ? "null" : entry.getValue().
          toString();
        refined.put(entry.getKey(), value);
      }
    }
    
    return refined;
  }
  
  public void setMap(Map<String, Object> map) {
    this.map = map;
  }
  
  public String getDocType() {
    return docType;
  }
  
  public void setDocType(String docType) {
    this.docType = docType;
  }
  
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public Integer getVersion() {
    return version;
  }
  
  public void setVersion(Integer version) {
    this.version = version;
  }
  
  public Integer getProjectId() {
    return projectId;
  }
  
  public void setProjectId(Integer projectId) {
    this.projectId = projectId;
  }
  
  public String getProjectName() {
    return projectName;
  }
  
  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }
  
  public Set<String> getFeatures() {
    return features;
  }
  
  public void setFeatures(Set<String> features) {
    this.features = features;
  }
  
  public Map<String, String> getTags() {
    return tags;
  }
  
  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }
  
  public Map<String, String> getOtherXAttrs() {
    return otherXAttrs;
  }
  
  public void setOtherXAttrs(Map<String, String> otherXAttrs) {
    this.otherXAttrs = otherXAttrs;
  }
}
