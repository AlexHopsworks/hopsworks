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

import io.hops.hopsworks.common.provenance.core.elastic.BasicElasticHit;
import io.hops.hopsworks.common.provenance.core.elastic.ProvElasticController;
import io.hops.hopsworks.common.provenance.ops.dto.ProvOpsElastic;
import io.hops.hopsworks.common.provenance.util.ProvHelper;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ElasticException;
import io.hops.hopsworks.exceptions.ProvenanceException;
import org.elasticsearch.action.get.GetRequest;

public class ProvOElasticHelper {
  public static ProvOpsElastic getFileOp(Long projectIId, String docId, Settings settings,
    ProvElasticController client)
    throws ProvenanceException {
    GetRequest request = new GetRequest(
      settings.getProvFileIndex(projectIId),
      docId);
    try {
      ProvOpsElastic result = client.getDoc(request, (BasicElasticHit hit) -> ProvOpsElastic.instance(hit));
      return result;
    } catch (ElasticException e) {
      String msg = "provenance - elastic query problem";
      throw ProvHelper.fromElastic(e, msg, msg + " - file ops");
    }
  }
}
