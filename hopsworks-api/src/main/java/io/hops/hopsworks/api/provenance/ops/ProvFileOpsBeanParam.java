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
package io.hops.hopsworks.api.provenance.ops;

import io.hops.hopsworks.api.provenance.ProvenanceResource;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.QueryParam;
import java.util.List;
import java.util.Set;

public class ProvFileOpsBeanParam {
  @QueryParam("filter_by")
  @ApiParam(value = "ex. filter_by=fileName:file1",
    allowableValues = "filter_by=PROJECT_I_ID:id1, filter_by=FILE_I_ID:id1, " +
      "filter_by=FILE_NAME:file1, filter_by=FILE_NAME_LIKE:fil, " +
      "filter_by=FILE_OPERATION:CREATE/DELETE/ACCESS_DATA/MODIFY_DATA" +
      "filter_by=USER_ID:user1, filter_by=APP_ID:app1," +
      "filter_by=TIMESTAMP:10030042, " +
      "filter_by=TIMESTAMP_LT:10030042, filter_by=TIMESTAMP_GT:10010042" +
      "filter_by=TIMESTAMP_LTE:10030042, filter_by=TIMESTAMP_GTE:10010042",
    
    allowMultiple = true)
  private Set<String> fileOpsFilterBy;
  
  @QueryParam("sort_by")
  @ApiParam(value = "ex. sort_by=CREATE_TIMESTAMP:asc",
    allowableValues = "sort_by=PROJECT_I_ID:asc, sort_by=FILE_I_ID:desc, sort_by=FILE_NAME:asc, " +
      "sort_by=FILE_OPERATION:asc, sort_by=USER_ID:asc, sort_by=APP_ID:asc, " +
      "sort_by=TIMESTAMP:asc",
    allowMultiple = true)
  private List<String> fileOpsSortBy;
  
  @QueryParam("expand")
  @ApiParam(value = "ex. expand=APP_STATE&expand=DIR_TREE", allowableValues = "expand=appState&expand=DIR_TREE")
  private Set<String> expansions;
  
  @QueryParam("app_filter_by")
  @ApiParam(value = "ex. app_filter_by=APP_STATE:state",
    allowableValues = "app_filter_by=APP_ID:appId, app_filter_by=APP_STATE:state",
    allowMultiple = true)
  private Set<String> appExpansionParams;
  
  @QueryParam("aggregations")
  @ApiParam(value = "ex. aggregations=FILES_IN, FILES_LEAST_ACTIVE_BY_LAST_ACCESSED",
    allowableValues = "aggregations=FILES_IN, FILES_LEAST_ACTIVE_BY_LAST_ACCESSED, " +
      "PROJECTS_LEAST_ACTIVE_BY_LAST_ACCESSED",
    allowMultiple = true)
  private Set<String> aggregations;
  
  @QueryParam("return_type")
  @DefaultValue("LIST")
  private ProvenanceResource.FileStructReturnType returnType;
  
  @QueryParam("compaction")
  @DefaultValue("NONE")
  private ProvenanceResource.FileOpsCompactionType opsCompaction;
  
  public ProvFileOpsBeanParam(
    @QueryParam("filter_by") Set<String> fileOpsFilterBy,
    @QueryParam("sort_by") List<String> fileOpsSortBy,
    @QueryParam("expand") Set<String> expansions,
    @QueryParam("app_filter_by") Set<String> appExpansionParams,
    @QueryParam("aggregations") Set<String> aggregations,
    @QueryParam("return_type") @DefaultValue("LIST") ProvenanceResource.FileStructReturnType returnType,
    @QueryParam("compaction") @DefaultValue("NONE") ProvenanceResource.FileOpsCompactionType opsCompaction) {
    
    this.fileOpsFilterBy = fileOpsFilterBy;
    this.fileOpsSortBy = fileOpsSortBy;
    this.expansions = expansions;
    this.appExpansionParams = appExpansionParams;
    this.aggregations = aggregations;
    this.returnType = returnType;
    this.opsCompaction = opsCompaction;
  }
  
  public Set<String> getFileOpsFilterBy() {
    return fileOpsFilterBy;
  }
  
  public void setFileOpsFilterBy(Set<String> fileOpsFilterBy) {
    this.fileOpsFilterBy = fileOpsFilterBy;
  }
  
  public List<String> getFileOpsSortBy() {
    return fileOpsSortBy;
  }
  
  public void setFileOpsSortBy(List<String> fileOpsSortBy) {
    this.fileOpsSortBy = fileOpsSortBy;
  }
  
  public Set<String> getExpansions() {
    return expansions;
  }
  
  public void setExpansions(Set<String> expansions) {
    this.expansions = expansions;
  }
  
  public Set<String> getAppExpansionParams() {
    return appExpansionParams;
  }
  
  public void setAppExpansionParams(Set<String> appExpansionParams) {
    this.appExpansionParams = appExpansionParams;
  }
  
  public Set<String> getAggregations() {
    return aggregations;
  }
  
  public void setAggregations(Set<String> aggregations) {
    this.aggregations = aggregations;
  }
  
  public ProvenanceResource.FileStructReturnType getReturnType() {
    return returnType;
  }
  
  public void setReturnType(ProvenanceResource.FileStructReturnType returnType) {
    this.returnType = returnType;
  }
  
  public ProvenanceResource.FileOpsCompactionType getOpsCompaction() {
    return opsCompaction;
  }
  
  public void setOpsCompaction(ProvenanceResource.FileOpsCompactionType opsCompaction) {
    this.opsCompaction = opsCompaction;
  }
  
  @Override
  public String toString() {
    return "ProvFileStateBeanParam{"
      + "file ops:" + fileOpsFilterBy.toString()
      + "ops compaction:" + opsCompaction
      + "return type:" + returnType
      + '}';
  }
}