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
package io.hops.hopsworks.api.project.alert;

import io.swagger.annotations.ApiParam;

import javax.ws.rs.QueryParam;
import java.util.LinkedHashSet;
import java.util.Set;

public class ProjectAlertsBeanParam {

  @QueryParam("sort_by")
  @ApiParam(value = "ex. sort_by=ID:asc,created:desc")
  private String sortBy;
  private final Set<ProjectAlertsSortBy> sortBySet;
  @QueryParam("filter_by")
  @ApiParam(value = "ex. filter_by=id:1", allowMultiple = true)
  private Set<ProjectAlertsFilterBy> filter;

  public ProjectAlertsBeanParam(@QueryParam("sort_by") String sortBy,
      @QueryParam("filter_by") Set<ProjectAlertsFilterBy> filter) {
    this.sortBy = sortBy;
    this.sortBySet = getSortBy(sortBy);
    this.filter = filter;

  }

  private Set<ProjectAlertsSortBy> getSortBy(String param) {
    if (param == null || param.isEmpty()) {
      return new LinkedHashSet<>();
    }
    String[] params = param.split(",");
    //Hash table and linked list implementation of the Set interface, with predictable iteration order
    Set<ProjectAlertsSortBy> sortBys = new LinkedHashSet<>();
    ProjectAlertsSortBy sort;
    for (String s : params) {
      sort = new ProjectAlertsSortBy(s.trim());
      sortBys.add(sort);
    }
    return sortBys;
  }

  public String getSortBy() {
    return sortBy;
  }

  public void setSortBy(String sortBy) {
    this.sortBy = sortBy;
  }

  public Set<ProjectAlertsFilterBy> getFilter() {
    return filter;
  }

  public void setFilter(Set<ProjectAlertsFilterBy> filter) {
    this.filter = filter;
  }

  public Set<ProjectAlertsSortBy> getSortBySet() {
    return sortBySet;
  }

}