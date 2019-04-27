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
package io.hops.hopsworks.common.provenance.v2;

import io.hops.hopsworks.common.elastic.ProvElasticHelper;
import io.hops.hopsworks.common.provenance.util.ElasticPaginationChecker;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class ProvFileOpsParamBuilder {
  private Map<String, ProvFileQuery.FilterVal> fileOpsFilterBy = new HashMap<>();
  private List<Pair<ProvFileQuery.Field, SortOrder>> fileOpsSortBy = new ArrayList<>();
  private Set<ProvFileQuery.FileExpansions> expansions = new HashSet<>();
  private Map<String, ProvFileQuery.FilterVal> appStateFilter = new HashMap<>();
  private Pair<Integer, Integer> pagination = null;
  private List<Script> filterScripts = new LinkedList<>();
  private List<ProvElasticHelper.ProvAggregations> aggregations = new LinkedList<>();
  
  public ProvFileOpsParamBuilder withQueryParamFilterBy(Set<String> params)
    throws GenericException {
    ProvParamBuilder.withFilterBy(fileOpsFilterBy, params, ProvFileQuery.QueryType.QUERY_FILE_OP);
    return this;
  }
  
  public ProvFileOpsParamBuilder withQueryParamSortBy(List<String> params) throws GenericException {
    for(String param : params) {
      fileOpsSortBy.add(ProvFileQuery.extractSort(param, ProvFileQuery.QueryType.QUERY_FILE_OP));
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder withQueryParamExpansions(Set<String> params) throws GenericException {
    ProvParamBuilder.withExpansions(expansions, params);
    return this;
  }
  
  public ProvFileOpsParamBuilder withQueryParamAppExpansionFilter(Set<String> params) throws GenericException {
    for(String param : params) {
      ProvParamBuilder.addToFilters(appStateFilter,
        ProvFileQuery.extractFilter(param, ProvFileQuery.QueryType.QUERY_EXPANSION_APP));
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder withPagination(Integer offset, Integer limit) throws GenericException {
    ElasticPaginationChecker.checkPagination(offset, limit);
    pagination = Pair.with(offset, limit);
    return this;
  }
  
  public boolean hasPagination() {
    return pagination != null;
  }
  
  public boolean hasAppExpansion() {
    return expansions.contains(ProvFileQuery.FileExpansions.APP);
  }
  
  public ProvFileOpsParamBuilder withAppExpansion() {
    expansions.add(ProvFileQuery.FileExpansions.APP);
    return this;
  }
  
  public ProvFileOpsParamBuilder withAppExpansion(String appId) throws GenericException {
    withAppExpansion();
    ProvParamBuilder.addToFilters(appStateFilter,
      Pair.with(ProvFileQuery.ExpansionApp.APP_ID, appId));
    return this;
  }
  
  public ProvFileOpsParamBuilder withProjectInodeId(Long projectInodeId) throws GenericException {
    ProvParamBuilder.addToFilters(fileOpsFilterBy, Pair.with(ProvFileQuery.FileOps.PROJECT_I_ID, projectInodeId));
    return this;
  }
  
  public ProvFileOpsParamBuilder withFileInodeId(Long fileInodeId) throws GenericException {
    ProvParamBuilder.addToFilters(fileOpsFilterBy, Pair.with(ProvFileQuery.FileOps.FILE_I_ID, fileInodeId));
    return this;
  }
  
  
  public ProvFileOpsParamBuilder withAppId(String appId) throws GenericException {
    ProvParamBuilder.addToFilters(fileOpsFilterBy, Pair.with(ProvFileQuery.FileOps.APP_ID, appId));
    return this;
  }
  
  public ProvFileOpsParamBuilder filterByFileOperation(ProvFileOps fileOp) throws GenericException {
    ProvParamBuilder.addToFilters(fileOpsFilterBy, Pair.with(ProvFileQuery.FileOps.FILE_OPERATION, fileOp.name()));
    return this;
  }
  
  public ProvFileOpsParamBuilder filterByScript(ProvElasticHelper.FilterByScripts script) {
    filterScripts.add(script.script);
    return this;
  }
  
  public ProvFileOpsParamBuilder withAggregation(ProvElasticHelper.ProvAggregations aggregation) {
    this.aggregations.add(aggregation);
    return this;
  }
  
  public ProvFileOpsParamBuilder withAggregations(Set<String> aggregations) throws GenericException {
    for(String agg : aggregations) {
      try {
        ProvElasticHelper.ProvAggregations aggregation = ProvElasticHelper.ProvAggregations.valueOf(agg);
        withAggregation(aggregation);
      } catch(NullPointerException | IllegalArgumentException e) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "aggregation" + agg + " not supported - supported:" + EnumSet.allOf(ProvElasticHelper.ProvAggregations.class),
          "exception extracting aggregations");
      }
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder sortBy(String field, SortOrder order) throws GenericException {
    ProvFileQuery.Field sortField = ProvFileQuery.extractBaseField(field, ProvFileQuery.QueryType.QUERY_FILE_OP);
    fileOpsSortBy.add(Pair.with(sortField, order));
    return this;
  }
  
  public ProvFileOpsParamBuilder sortByField(ProvElasticFields.Field field, SortOrder order) throws GenericException {
    ProvFileQuery.Field sortField
      = ProvFileQuery.extractBaseField(field.toString().toLowerCase(), ProvFileQuery.QueryType.QUERY_FILE_OP);
    fileOpsSortBy.add(Pair.with(sortField, order));
    return this;
  }
  
  public ProvFileOpsParamBuilder filterByField(ProvFileQuery.Field field, String val) throws GenericException {
    if(!(field instanceof ProvFileQuery.FileOps
      || field instanceof ProvFileQuery.FileOpsAux)) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "allowed fields - FileOps and fileOpsAux");
    }
    Object v = field.filterValParser().parse(val);
    ProvParamBuilder.addToFilters(fileOpsFilterBy, Pair.with(field, v));
    return this;
  }
  public Map<String, ProvFileQuery.FilterVal> getFileOpsFilterBy() {
    return fileOpsFilterBy;
  }
  
  public List<Pair<ProvFileQuery.Field, SortOrder>> getFileOpsSortBy() {
    return fileOpsSortBy;
  }
  
  public Map<String, ProvFileQuery.FilterVal> getAppStateFilter() {
    return appStateFilter;
  }
  
  public Pair<Integer, Integer> getPagination() {
    return pagination;
  }
  
  public List<Script> getFilterScripts() {
    return filterScripts;
  }
  
  public List<ProvElasticHelper.ProvAggregations> getAggregations() {
    return aggregations;
  }
  
  public boolean hasFileOpFilters() {
    return fileOpsFilterBy.containsKey(ProvFileQuery.FileOps.FILE_OPERATION.queryFieldName());
  }
}
