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

import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.common.provenance.util.ElasticPaginationChecker;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class ProvFileStateParamBuilder {
  private Map<String, ProvFileQuery.FilterVal> fileStateFilter = new HashMap<>();
  private List<Pair<ProvFileQuery.Field, SortOrder>> fileStateSortBy = new ArrayList<>();
  private Map<String, String> exactXAttrFilter = new HashMap<>();
  private Map<String, String> likeXAttrFilter = new HashMap<>();
  private Set<String> hasXAttrFilter = new HashSet<>();
  private List<SortE> xAttrSortBy = new ArrayList<>();
  private Set<ProvFileQuery.FileExpansions> expansions = new HashSet<>();
  private Map<String, ProvFileQuery.FilterVal> appStateFilter = new HashMap<>();
  private Pair<Integer, Integer> pagination = null;
  
  public ProvFileStateParamBuilder withQueryParamFileStateFilterBy(Set<String> params) throws GenericException {
    ProvParamBuilder.withFilterBy(fileStateFilter, params, ProvFileQuery.QueryType.QUERY_FILE_STATE);
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamExactXAttr(Set<String> params) throws GenericException {
    for(String param : params) {
      Pair<String, String> p = ProvFileQuery.extractXAttrParam(param);
      exactXAttrFilter.put(p.getValue0(), p.getValue1());
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamLikeXAttr(Set<String> params) throws GenericException {
    for(String param : params) {
      Pair<String, String> p = ProvFileQuery.extractXAttrParam(param);
      likeXAttrFilter.put(p.getValue0(), p.getValue1());
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamExpansions(Set<String> params) throws GenericException {
    ProvParamBuilder.withExpansions(expansions, params);
    return this;
  }
  
  public ProvFileStateParamBuilder withQueryParamAppExpansionFilter(Set<String> params) throws GenericException {
    for(String param : params) {
      ProvParamBuilder.addToFilters(appStateFilter,
        ProvFileQuery.extractFilter(param, ProvFileQuery.QueryType.QUERY_EXPANSION_APP));
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withPagination(Integer offset, Integer limit) throws GenericException {
    ElasticPaginationChecker.checkPagination(offset,  limit);
    pagination = Pair.with(offset, limit);
    return this;
  }
  
  public Map<String, ProvFileQuery.FilterVal> getFileStateFilter() {
    return fileStateFilter;
  }
  
  public List<Pair<ProvFileQuery.Field, SortOrder>> getFileStateSortBy() {
    return fileStateSortBy;
  }
  
  public Map<String, String> getExactXAttrFilter() {
    return exactXAttrFilter;
  }
  
  public Map<String, String> getLikeXAttrFilter() {
    return likeXAttrFilter;
  }
  
  public Set<String> getHasXAttrFilter() {
    return hasXAttrFilter;
  }
  
  public List<SortE> getXAttrSortBy() {
    return xAttrSortBy;
  }
  
  public Set<ProvFileQuery.FileExpansions> getExpansions() {
    return expansions;
  }
  
  public Map<String, ProvFileQuery.FilterVal> getAppStateFilter() {
    return appStateFilter;
  }
  
  public Pair<Integer, Integer> getPagination() {
    return pagination;
  }
  
  public ProvFileStateParamBuilder withProjectInodeId(Long projectInodeId) throws GenericException {
    ProvParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvFileQuery.FileState.PROJECT_I_ID, projectInodeId));
    return this;
  }
  
  public ProvFileStateParamBuilder withFileInodeId(Long inodeId) throws GenericException {
    ProvParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvFileQuery.FileState.FILE_I_ID, inodeId));
    return this;
  }
  
  public ProvFileStateParamBuilder withFileName(String fileName) throws GenericException {
    ProvParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvFileQuery.FileState.FILE_NAME, fileName));
    return this;
  }
  
  public ProvFileStateParamBuilder withFileNameLike(String fileName) throws GenericException {
    ProvParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvFileQuery.FileStateAux.FILE_NAME_LIKE, fileName));
    return this;
  }
  
  public ProvFileStateParamBuilder withUserId(String userId) throws GenericException {
    ProvParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvFileQuery.FileState.USER_ID, userId));
    return this;
  }
  
  public ProvFileStateParamBuilder createdBefore(Long timestamp) throws GenericException {
    ProvParamBuilder.addToFilters(fileStateFilter,
      Pair.with(ProvFileQuery.FileStateAux.CREATE_TIMESTAMP_LT, timestamp));
    return this;
  }
  
  public ProvFileStateParamBuilder createdAfter(Long timestamp) throws GenericException {
    ProvParamBuilder.addToFilters(fileStateFilter,
      Pair.with(ProvFileQuery.FileStateAux.CREATE_TIMESTAMP_GT, timestamp));
    return this;
  }
  
  public ProvFileStateParamBuilder createdOn(Long timestamp) throws GenericException {
    ProvParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvFileQuery.FileState.CREATE_TIMESTAMP, timestamp));
    return this;
  }
  
  public ProvFileStateParamBuilder withAppId(String appId) throws GenericException {
    ProvParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvFileQuery.FileState.APP_ID, appId));
    return this;
  }
  
  public ProvFileStateParamBuilder withMlId(String mlId) throws GenericException {
    ProvParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvFileQuery.FileState.ML_ID, mlId));
    return this;
  }
  
  public ProvFileStateParamBuilder withMlType(String mlType) throws GenericException {
    ProvParamBuilder.addToFilters(fileStateFilter, Pair.with(ProvFileQuery.FileState.ML_TYPE, mlType));
    return this;
  }
  
  public ProvFileStateParamBuilder withXAttrs(Map<String, String> xAttrs) {
    for(Map.Entry<String, String> xAttr : xAttrs.entrySet()) {
      withXAttr(xAttr.getKey(), xAttr.getValue());
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withXAttr(String key, String val) {
    String xattrKey = ProvFileQuery.processXAttrKey(key);
    exactXAttrFilter.put(xattrKey, val);
    return this;
  }
  
  public ProvFileStateParamBuilder withXAttrsLike(Map<String, String> xAttrs) {
    for(Map.Entry<String, String> xAttr : xAttrs.entrySet()) {
      withXAttrLike(xAttr.getKey(), xAttr.getValue());
    }
    return this;
  }
  
  public ProvFileStateParamBuilder withXAttrLike(String key, String val) {
    String xattrKey = ProvFileQuery.processXAttrKey(key);
    likeXAttrFilter.put(xattrKey, val);
    return this;
  }
  
  public ProvFileStateParamBuilder withAppExpansion() {
    expansions.add(ProvFileQuery.FileExpansions.APP);
    return this;
  }
  
  public ProvFileStateParamBuilder withAppExpansionCurrentState(Provenance.AppState currentAppState)
    throws GenericException {
    withAppExpansion();
    ProvParamBuilder.addToFilters(appStateFilter,
      Pair.with(ProvFileQuery.ExpansionApp.APP_STATE, currentAppState.name()));
    return this;
  }
  
  public ProvFileStateParamBuilder withAppExpansion(String appId) throws GenericException {
    withAppExpansion();
    ProvParamBuilder.addToFilters(appStateFilter,
      Pair.with(ProvFileQuery.ExpansionApp.APP_ID, appId));
    return this;
  }

  public ProvFileStateParamBuilder withQueryParamFileStateSortBy(List<String> params) throws GenericException {
    for(String param : params) {
      fileStateSortBy.add(ProvFileQuery.extractSort(param, ProvFileQuery.QueryType.QUERY_FILE_STATE));
    }
    return this;
  }

  public ProvFileStateParamBuilder withQueryParamXAttrSortBy(List<String> params) throws GenericException {
    for(String param : params) {
      Pair<String, String> xattr = ProvFileQuery.extractXAttrParam(param);
      SortOrder order = ProvFileQuery.extractSortOrder(xattr.getValue1());
      xAttrSortBy.add(new SortE(xattr.getValue0(), order));
    }
    return this;
  }
  
  public ProvFileStateParamBuilder sortBy(String field, SortOrder order) {
    try {
      ProvFileQuery.Field sortField = ProvFileQuery.extractBaseField(field, ProvFileQuery.QueryType.QUERY_FILE_STATE);
      fileStateSortBy.add(Pair.with(sortField, order));
    } catch(GenericException ex) {
      String xattrKey = ProvFileQuery.processXAttrKey(field);
      xAttrSortBy.add(new SortE(xattrKey, order));
    }
    return this;
  }
  
  public void fixSortBy(String index, Map<String, String> mapping) throws ServiceException {
    for(SortE s : xAttrSortBy) {
      String type = mapping.get(s.key);
      if(type == null) {
        String devMsg = "index[" + index + "] - error querying - index missing mapping for field[" + s.key + "]";
        String usrMsg = "elastic query error - bad field[" + s.key + "]";
        ServiceException ex = new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_NO_MAPPING,
          Level.INFO, usrMsg, devMsg);
        throw ex;
      }
      if(type.equals("text")) {
        s.key = s.key + ".keyword";
      }
    }
  }
  
  public ProvFileStateParamBuilder filterByStateField(ProvFileQuery.Field field, String val) throws GenericException {
    if(!(field instanceof ProvFileQuery.FileState
      || field instanceof ProvFileQuery.FileStateAux)) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "allowed fields - FileState and fileStateAux");
    }
    Object v = field.filterValParser().parse(val);
    ProvParamBuilder.addToFilters(fileStateFilter, Pair.with(field, v));
    return this;
  }
  
  public ProvFileStateParamBuilder filterByHasXAttr(String key) {
    String xattrKey = ProvFileQuery.processXAttrKey(key);
    hasXAttrFilter.add(xattrKey);
    return this;
  }
  
  public ProvFileStateParamBuilder filterByHasXAttr(Set<String> keys) {
    for(String key : keys) {
      String xattrKey = ProvFileQuery.processXAttrKey(key);
      hasXAttrFilter.add(xattrKey);
    }
    return this;
  }
  
  public boolean hasAppExpansion() {
    return expansions.contains(ProvFileQuery.FileExpansions.APP);
  }
  
  public static class SortE {
    public String key;
    public SortOrder order;
    
    public SortE(String key, SortOrder order) {
      this.key = key;
      this.order = order;
    }
  }
}
