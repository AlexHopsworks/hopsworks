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
package io.hops.hopsworks.common.provenance.state.dto;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.annotation.XmlRootElement;

import io.hops.hopsworks.common.provenance.core.elastic.BasicElasticHit;
import io.hops.hopsworks.common.provenance.core.Provenance;

/**
 * Represents a JSONifiable version of the elastic hit object
 */
@XmlRootElement
public class ProvAppStateElastic implements Comparator<ProvAppStateElastic> {

  private static final Logger LOG = Logger.getLogger(ProvAppStateElastic.class.getName());
  public static final String APP_ID_FIELD = "app_id";
  public static final String APP_STATE_FIELD = "app_state";
  public static final String TIMESTAMP_FIELD = "timestamp";
  public static final String APP_NAME_FIELD = "app_name";
  public static final String APP_USER_FIELD = "app_user";
  public static final String READABLE_TIMESTAMP_FIELD = "readable_timestamp";

  private String id;
  private float score;
  private Map<String, Object> map;

  private String appId;
  private Provenance.AppState appState = null;
  private long appStateTimestamp;
  private String readableTimestamp;
  private String appName;
  private String appUser;

  public ProvAppStateElastic() {
  }

  public ProvAppStateElastic(BasicElasticHit hit) {
    this.id = hit.getId();
    this.score = hit.getScore();
    //the source of the retrieved record (i.e. all the indexed information)
    this.map = hit.getSource();

    //export the name of the retrieved record from the list
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      //set the name explicitly so that it's easily accessible in the frontend
      switch (entry.getKey()) {
        case APP_ID_FIELD:
          this.appId = entry.getValue().toString();
          break;
        case APP_STATE_FIELD:
          if (entry.getValue() != null) {
            try {
              this.appState = Provenance.AppState.valueOf(entry.getValue().toString());
            } catch (IllegalArgumentException ex) {
              this.appState = Provenance.AppState.UNKNOWN;
            }
          } else {
            this.appState = Provenance.AppState.UNKNOWN;
          }
          break;
        case TIMESTAMP_FIELD:
          this.appStateTimestamp = ((Number) entry.getValue()).longValue();
          break;
        case APP_NAME_FIELD:
          this.appName = entry.getValue().toString();
          break;
        case APP_USER_FIELD:
          this.appUser = entry.getValue().toString();
          break;
        case READABLE_TIMESTAMP_FIELD:
          this.readableTimestamp = entry.getValue().toString();
          break;
        default:
          LOG.log(Level.WARNING, "unknown key:{0}", new Object[]{entry.getKey()});
          break;
      }
    }
  }

  @Override
  public int compare(ProvAppStateElastic o1, ProvAppStateElastic o2) {
    return Float.compare(o2.getScore(), o1.getScore());
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

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public Provenance.AppState getAppState() {
    return appState;
  }

  public void setAppState(Provenance.AppState appState) {
    this.appState = appState;
  }

  public Long getAppStateTimestamp() {
    return appStateTimestamp;
  }

  public void setAppStateTimestamp(long appStateTimestamp) {
    this.appStateTimestamp = appStateTimestamp;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public String getAppUser() {
    return appUser;
  }

  public void setAppUser(String appUser) {
    this.appUser = appUser;
  }
}
