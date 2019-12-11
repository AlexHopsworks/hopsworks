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
package io.hops.hopsworks.common.provenance.util;

import io.hops.hopsworks.common.provenance.core.apiToElastic.ProvParser;
import io.hops.hopsworks.common.provenance.core.elastic.BasicElasticHit;
import io.hops.hopsworks.common.provenance.core.elastic.ElasticClient;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpElastic;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.exceptions.ServiceException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;

import java.util.Map;

public class ProvElasticHelper {
  public static BoolQueryBuilder filterByBasicFields(BoolQueryBuilder query,
    Map<String, ProvParser.FilterVal> filters) throws ProvenanceException {
    for (Map.Entry<String, ProvParser.FilterVal> fieldFilters : filters.entrySet()) {
      query.must(fieldFilters.getValue().query());
    }
    return query;
  }
  
  public static ProvFileOpElastic getFileOp(Long projectIId, String docId, Settings settings,
    ElasticClient client)
    throws ServiceException, ProvenanceException {
    GetRequest request = new GetRequest(
      settings.getProvFileIndex(projectIId),
      Settings.PROV_FILE_DOC_TYPE,
      docId);
    ProvFileOpElastic result =  client.getDoc(request,
      (BasicElasticHit hit) -> ProvFileOpElastic.instance(hit));
    return result;
  }
}
