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

import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.javatuples.Pair;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class ProvParamBuilder {
  public static void addToFilters(Map<String, ProvFileQuery.FilterVal> filters,
    Pair<ProvFileQuery.Field, Object> filter)
    throws GenericException {
    ProvFileQuery.FilterVal fieldFilters = filters.get(filter.getValue0().queryFieldName());
    if(fieldFilters == null) {
      fieldFilters = ProvFileQuery.filterValInstance(filter.getValue0().filterType());
      filters.put(filter.getValue0().queryFieldName(), fieldFilters);
    }
    fieldFilters.add(filter);
  }
  
  public static void withExpansions(Set<ProvFileQuery.FileExpansions> expansions, Set<String> params)
    throws GenericException {
    for(String param : params) {
      try {
        expansions.add(ProvFileQuery.FileExpansions.valueOf(param));
      } catch (NullPointerException | IllegalArgumentException e) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "param " + param + " not supported - supported params:"
            + EnumSet.allOf(ProvFileQuery.FileExpansions.class),
          "exception extracting FilterBy param", e);
      }
    }
  }
  
  public static void withFilterBy(Map<String, ProvFileQuery.FilterVal> filters, Set<String> params,
    ProvFileQuery.QueryType queryType) throws GenericException {
    for(String param : params) {
      addToFilters(filters, ProvFileQuery.extractFilter(param, queryType));
    }
  }
}
