/*
 * Changes to this file committed after and not including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
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
 *
 * Changes to this file committed before and including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
 * Copyright (C) 2013 - 2018, Logical Clocks AB and RISE SICS AB. All rights reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS  OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package io.hops.hopsworks.common.provenance;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.annotation.XmlRootElement;
import org.elasticsearch.search.SearchHit;

/**
 * Represents a JSONifiable version of the elastic hit object
 */
@XmlRootElement
public class FileProvenanceHit implements Comparator<FileProvenanceHit> {

  private static final Logger LOG = Logger.getLogger(FileProvenanceHit.class.getName());
  public static final String INODE_ID_FIELD = "inode_id";
  public static final String INODE_OPERATION_NAME_FIELD = "inode_operation";
  public static final String APP_ID_FIELD = "io_app_id";
  public static final String USER_ID_FIELD = "io_user_id";
  public static final String PROJECT_INODE_ID_FIELD = "project_i_id";
  public static final String DATASET_INODE_ID_FIELD = "dataset_i_id"; 
  public static final String INODE_NAME_FIELD = "i_name";
  public static final String TIMESTAMP_FIELD = "i_readable_t";
  private String id;
  private float score;
  private Map<String, Object> map;

  private long inodeId;
  private String inodeOperation;
  private String appId;
  private int userId;
  private long projectInodeId;
  private long datasetInodeId;
  private String inodeName;
  private String timestamp;
  private String mlType;
  private String mlId;
  private String mlDeps;

  public FileProvenanceHit() {
  }

  public FileProvenanceHit(SearchHit hit) {
    this.id = hit.getId();
    this.score = hit.getScore();
    //the source of the retrieved record (i.e. all the indexed information)
    this.map = hit.getSourceAsMap();

    //export the name of the retrieved record from the list
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      //set the name explicitly so that it's easily accessible in the frontend
      switch (entry.getKey()) {
        case INODE_ID_FIELD:
          this.inodeId = ((Number) entry.getValue()).longValue();
          break;
        case INODE_OPERATION_NAME_FIELD:
          this.inodeOperation = entry.getValue().toString();
          break;
        case APP_ID_FIELD:
          this.appId = entry.getValue().toString();
          break;
        case USER_ID_FIELD:
          this.userId = ((Number) entry.getValue()).intValue();
          break;
        case PROJECT_INODE_ID_FIELD:
          this.projectInodeId = ((Number) entry.getValue()).longValue();
          break;
        case DATASET_INODE_ID_FIELD:
          this.datasetInodeId = ((Number) entry.getValue()).longValue();
          break;
        case INODE_NAME_FIELD:
          this.inodeName = entry.getValue().toString();
          break;
        case TIMESTAMP_FIELD:
          this.timestamp = entry.getValue().toString();
          break;
        default:
          LOG.log(Level.WARNING, "unknown key:{0}", new Object[]{entry.getKey()});
          break;
      }
    }
  }
  
  @Override
  public int compare(FileProvenanceHit o1, FileProvenanceHit o2) {
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
  
  public void setMap(Map<String, Object> source) {
    this.map = new HashMap<>(source);
  }

  public Map<String, String> getMap() {
    //flatten hits (remove nested json objects) to make it more readable
    Map<String, String> refined = new HashMap<>();

    if (this.map != null) {
      for (Map.Entry<String, Object> entry : this.map.entrySet()) {
        //convert value to string
        String value = (entry.getValue() == null) ? "null" : entry.getValue().toString();
        refined.put(entry.getKey(), value);
      }
    }

    return refined;
  }

  public long getInodeId() {
    return inodeId;
  }

  public void setInodeId(long inodeId) {
    this.inodeId = inodeId;
  }

  public String getInodeOperation() {
    return inodeOperation;
  }

  public void setInodeOperation(String inodeOperation) {
    this.inodeOperation = inodeOperation;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public int getUserId() {
    return userId;
  }

  public void setUserId(int userId) {
    this.userId = userId;
  }

  public long getProjectInodeId() {
    return projectInodeId;
  }

  public void setProjectInodeId(long projectInodeId) {
    this.projectInodeId = projectInodeId;
  }

  public long getDatasetInodeId() {
    return datasetInodeId;
  }

  public void setDatasetInodeId(long datasetInodeId) {
    this.datasetInodeId = datasetInodeId;
  }

  public String getInodeName() {
    return inodeName;
  }

  public void setInodeName(String inodeName) {
    this.inodeName = inodeName;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public String getMlType() {
    return mlType;
  }

  public void setMlType(String mlType) {
    this.mlType = mlType;
  }

  public String getMlId() {
    return mlId;
  }

  public void setMlId(String mlId) {
    this.mlId = mlId;
  }

  public String getMlDeps() {
    return mlDeps;
  }

  public void setMlDeps(String mlDeps) {
    this.mlDeps = mlDeps;
  }
}
