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
package io.hops.hopsworks.common.provenance.ops;

import io.hops.hopsworks.common.provenance.app.ProvAParser;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.core.ProvParser;
import io.hops.hopsworks.common.provenance.ops.apiToElastic.ProvOParser;
import io.hops.hopsworks.exceptions.ProvenanceException;
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
  private Map<String, ProvParser.FilterVal> fileOpsFilterBy = new HashMap<>();
  private List<Pair<ProvParser.Field, SortOrder>> fileOpsSortBy = new ArrayList<>();
  private Set<ProvParser.Expansions> expansions = new HashSet<>();
  private Map<String, ProvParser.FilterVal> appStateFilter = new HashMap<>();
  private Pair<Integer, Integer> pagination = null;
  private List<Script> filterScripts = new LinkedList<>();
  private Set<ProvElasticAggregations.ProvAggregations> aggregations = new HashSet<>();
  
  public ProvFileOpsParamBuilder withQueryParamFilterBy(Set<String> params)
    throws ProvenanceException {
    for(String param : params) {
      ProvParser.addToFilters(fileOpsFilterBy, ProvOParser.extractFilter(param));
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder withQueryParamSortBy(List<String> params) throws ProvenanceException {
    for(String param : params) {
      fileOpsSortBy.add(ProvOParser.extractSort(param));
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder withQueryParamExpansions(Set<String> params) throws ProvenanceException {
    ProvParser.withExpansions(expansions, params);
    return this;
  }
  
  public ProvFileOpsParamBuilder withQueryParamAppExpansionFilter(Set<String> params) throws ProvenanceException {
    for(String param : params) {
      ProvParser.addToFilters(appStateFilter, ProvAParser.extractFilter(param));
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder withPagination(Integer offset, Integer limit) throws ProvenanceException {
    pagination = Pair.with(offset, limit);
    return this;
  }
  
  public boolean hasPagination() {
    return pagination != null;
  }
  
  public boolean hasAppExpansion() {
    return expansions.contains(ProvParser.Expansions.APP);
  }
  
  public ProvFileOpsParamBuilder withAppExpansion() {
    expansions.add(ProvParser.Expansions.APP);
    return this;
  }
  
  public ProvFileOpsParamBuilder withAppExpansion(String appId) throws ProvenanceException {
    withAppExpansion();
    ProvParser.addToFilters(appStateFilter, Pair.with(ProvAParser.Field.APP_ID, appId));
    return this;
  }
  
  public ProvFileOpsParamBuilder withProjectInodeId(Long projectInodeId) throws ProvenanceException {
    ProvParser.addToFilters(fileOpsFilterBy, Pair.with(ProvOParser.Fields.PROJECT_I_ID, projectInodeId));
    return this;
  }
  
  public ProvFileOpsParamBuilder withFileInodeId(Long fileInodeId) throws ProvenanceException {
    ProvParser.addToFilters(fileOpsFilterBy, Pair.with(ProvOParser.Fields.FILE_I_ID, fileInodeId));
    return this;
  }
  
  
  public ProvFileOpsParamBuilder withAppId(String appId) throws ProvenanceException {
    ProvParser.addToFilters(fileOpsFilterBy, Pair.with(ProvOParser.Fields.APP_ID, appId));
    return this;
  }
  
  public ProvFileOpsParamBuilder filterByFileOperation(Provenance.FileOps fileOp) throws ProvenanceException {
    ProvParser.addToFilters(fileOpsFilterBy, Pair.with(ProvOParser.Fields.FILE_OPERATION, fileOp.name()));
    return this;
  }
  
  public ProvFileOpsParamBuilder filterByScript(ProvElasticFilterScripts.Scripts script) {
    filterScripts.add(script.script);
    return this;
  }
  
  public ProvFileOpsParamBuilder withAggregation(ProvElasticAggregations.ProvAggregations aggregation) {
    this.aggregations.add(aggregation);
    return this;
  }
  
  public ProvFileOpsParamBuilder withAggregations(Set<String> aggregations) throws ProvenanceException {
    for(String agg : aggregations) {
      try {
        ProvElasticAggregations.ProvAggregations aggregation = ProvElasticAggregations.ProvAggregations.valueOf(agg);
        withAggregation(aggregation);
      } catch(NullPointerException | IllegalArgumentException e) {
        String msg = "aggregation" + agg
          + " not supported - supported:" + EnumSet.allOf(ProvElasticAggregations.ProvAggregations.class);
        throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.BAD_REQUEST, Level.INFO,
          msg, "exception extracting aggregations");
      }
    }
    return this;
  }
  
  public ProvFileOpsParamBuilder sortBy(String field, SortOrder order) throws ProvenanceException {
    ProvParser.Field sortField = ProvOParser.extractField(field);
    fileOpsSortBy.add(Pair.with(sortField, order));
    return this;
  }
  
  public ProvFileOpsParamBuilder sortByField(ProvParser.ElasticField field, SortOrder order)
    throws ProvenanceException {
    ProvParser.Field sortField = ProvOParser.extractField(field.toString().toLowerCase());
    fileOpsSortBy.add(Pair.with(sortField, order));
    return this;
  }
  
  public ProvFileOpsParamBuilder filterByField(ProvParser.Field field, String val) throws ProvenanceException {
    if(!(field instanceof ProvOParser.Fields
      || field instanceof ProvOParser.AuxFields)) {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.BAD_REQUEST, Level.INFO,
        "allowed fields - FileOps and fileOpsAux");
    }
    Object v = field.filterValParser().apply(val);
    ProvParser.addToFilters(fileOpsFilterBy, Pair.with(field, v));
    return this;
  }
  public Map<String, ProvParser.FilterVal> getFileOpsFilterBy() {
    return fileOpsFilterBy;
  }
  
  public List<Pair<ProvParser.Field, SortOrder>> getFileOpsSortBy() {
    return fileOpsSortBy;
  }
  
  public Map<String, ProvParser.FilterVal> getAppStateFilter() {
    return appStateFilter;
  }
  
  public Pair<Integer, Integer> getPagination() {
    return pagination;
  }
  
  public List<Script> getFilterScripts() {
    return filterScripts;
  }
  
  public Set<ProvElasticAggregations.ProvAggregations> getAggregations() {
    return aggregations;
  }
  
  public boolean hasFileOpFilters() {
    return fileOpsFilterBy.containsKey(ProvOParser.Fields.FILE_OPERATION.queryFieldName());
  }
}
