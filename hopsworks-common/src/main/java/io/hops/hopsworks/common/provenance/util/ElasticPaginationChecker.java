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
package io.hops.hopsworks.common.provenance.util;

import io.hops.hopsworks.common.elastic.HopsworksElasticClient;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;
import java.util.logging.Level;

public class ElasticPaginationChecker {
  public static void checkPagination(Integer offset, Integer limit) throws GenericException {
    if(offset == null) {
      offset = 0;
    }
    if(offset < 0 || offset > HopsworksElasticClient.MAX_PAGE_SIZE) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "malformed - offset not between 0 and MAX_PAGE_SIZE:" + HopsworksElasticClient.MAX_PAGE_SIZE);
    }
    if(limit != null) {
      if(0 > limit || limit > HopsworksElasticClient.DEFAULT_PAGE_SIZE) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "malformed - limit not between 0 and DEFAULT_PAGE_SIZE:" + HopsworksElasticClient.DEFAULT_PAGE_SIZE);
      }
      if(limit > HopsworksElasticClient.MAX_PAGE_SIZE - offset) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "malformed - offset + limit exceed MAX_PAGE_SIZE:" + HopsworksElasticClient.MAX_PAGE_SIZE);
      }
    }
  }
}
