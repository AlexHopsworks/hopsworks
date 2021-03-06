/*
 * This file is part of Hopsworks
 * Copyright (C) 2021, Logical Clocks AB. All rights reserved
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

package io.hops.hopsworks.api.alert;

import javax.xml.bind.annotation.XmlRootElement;
import java.net.URL;
import java.util.List;

@XmlRootElement
public class PostableAlertDTO {
  private List<Entry> labels;
  private List<Entry> annotations;
  private String startsAt;
  private String endsAt;
  private URL generatorURL;

  public PostableAlertDTO() {
  }

  public List<Entry> getLabels() {
    return labels;
  }

  public void setLabels(List<Entry> labels) {
    this.labels = labels;
  }

  public List<Entry> getAnnotations() {
    return annotations;
  }

  public void setAnnotations(List<Entry> annotations) {
    this.annotations = annotations;
  }

  public String getStartsAt() {
    return startsAt;
  }

  public void setStartsAt(String startsAt) {
    this.startsAt = startsAt;
  }

  public String getEndsAt() {
    return endsAt;
  }

  public void setEndsAt(String endsAt) {
    this.endsAt = endsAt;
  }

  public URL getGeneratorURL() {
    return generatorURL;
  }

  public void setGeneratorURL(URL generatorURL) {
    this.generatorURL = generatorURL;
  }

  @Override
  public String toString() {
    return "PostableAlertDTO{" +
        "labels=" + labels +
        ", annotations=" + annotations +
        ", startsAt=" + startsAt +
        ", endsAt=" + endsAt +
        ", generatorURL=" + generatorURL +
        '}';
  }
}
