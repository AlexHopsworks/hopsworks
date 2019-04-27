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
package io.hops.hopsworks.api.provenance;

import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.provenance.xml.ProvFileOpsCompactByApp;
import io.hops.hopsworks.common.provenance.xml.ProvFileOpsSummaryByApp;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.xml.ProvFileOpDTO;
import io.hops.hopsworks.common.provenance.xml.ProvFileStateDTO;
import io.hops.hopsworks.common.provenance.xml.ProvFileStateTreeDTO;
import io.hops.hopsworks.common.provenance.xml.ProvFileStateMinDTO;
import io.hops.hopsworks.common.provenance.elastic.ProvenanceController;
import io.hops.hopsworks.common.provenance.elastic.ProvFootprintFileStatesElastic;
import io.hops.hopsworks.common.provenance.xml.WrapperDTO;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvFileOpsParamBuilder;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.elastic.ProvFootprintFileStateTreeElastic;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.javatuples.Pair;

import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProvenanceResourceHelper {
  private static final Logger LOG = Logger.getLogger(ProvenanceResourceHelper.class.getName());
  
  public static Response getFileStates(ProvenanceController provenanceCtrl, Project project,
    ProvFileStateParamBuilder params, ProjectProvenanceResource.FileStructReturnType returnType)
    throws ProvenanceException, ServiceException {
    switch (returnType) {
      case LIST:
        ProvFileStateDTO.PList listResult = provenanceCtrl.provFileStateList(project, params);
        return Response.ok().entity(listResult).build();
      case MIN_TREE:
        Pair<Map<Long, ProvFileStateTreeDTO>, Map<Long, ProvFileStateTreeDTO>> minAux
          = provenanceCtrl.provFileStateTree(project, params, false);
        ProvFileStateDTO.MinTree minTreeResult
          = new ProvFileStateDTO.MinTree(minAux.getValue0().values());
        return Response.ok().entity(minTreeResult).build();
      case FULL_TREE:
        Pair<Map<Long, ProvFileStateTreeDTO>, Map<Long, ProvFileStateTreeDTO>> fullAux
          = provenanceCtrl.provFileStateTree(project, params, true);
        ProvFileStateDTO.FullTree fullTreeResult
          = new ProvFileStateDTO.FullTree(fullAux.getValue0().values(), fullAux.getValue1().values());
        return Response.ok().entity(fullTreeResult).build();
      case COUNT:
        Long countResult = provenanceCtrl.provFileStateCount(project, params);
        return Response.ok().entity(new WrapperDTO<>(countResult)).build();
      default:
        throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
          "return type: " + returnType + " is not managed");
    }
  }
  
  public static Response getAppFootprint(ProvenanceController provenanceCtrl, Project project,
    ProvFileOpsParamBuilder params, Provenance.FootprintType footprintType,
    ProjectProvenanceResource.FileStructReturnType returnType)
    throws ProvenanceException, ServiceException {
    switch(returnType) {
      case LIST:
        List<ProvFileStateMinDTO> listAux
          = provenanceCtrl.provAppFootprintList(project, params, footprintType);
        ProvFootprintFileStatesElastic.PList listResult = new ProvFootprintFileStatesElastic.PList(listAux);
        return Response.ok().entity(listResult).build();
      case MIN_TREE:
        Pair<Map<Long, ProvFootprintFileStateTreeElastic>, Map<Long, ProvFootprintFileStateTreeElastic>> minAux
          = provenanceCtrl.provAppFootprintTree(project, params, footprintType, false);
        ProvFootprintFileStatesElastic.MinTree minTreeResult
          = new ProvFootprintFileStatesElastic.MinTree(minAux.getValue0().values());
        return Response.ok().entity(minTreeResult).build();
      case FULL_TREE:
        Pair<Map<Long, ProvFootprintFileStateTreeElastic>, Map<Long, ProvFootprintFileStateTreeElastic>> fullAux
          = provenanceCtrl.provAppFootprintTree(project, params, footprintType,true);
        ProvFootprintFileStatesElastic.FullTree fullTreeResult
          = new ProvFootprintFileStatesElastic.FullTree(fullAux.getValue0().values(), fullAux.getValue1().values());
        return Response.ok().entity(fullTreeResult).build();
      case ARTIFACTS:
        ProvFileOpDTO.Count aux = provenanceCtrl.provAppArtifactFootprint(project, params);
        return Response.ok().entity(aux).build();
      case COUNT:
      default:
        throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
          "return type: " + returnType + " is not managed");
    }
  }
  
  
  public static Response getFileOps(ProvenanceController provenanceCtrl, Project project,
    ProvFileOpsParamBuilder params, ProjectProvenanceResource.FileOpsCompactionType opsCompaction,
    ProjectProvenanceResource.FileStructReturnType returnType)
    throws ServiceException, ProvenanceException {
    switch(returnType) {
      case COUNT: {
        ProvFileOpDTO.Count result = provenanceCtrl.provFileOpsCount(project, params);
        return Response.ok().entity(result).build();
      }
      case ARTIFACTS: {
        ProvFileOpDTO.PList result = provenanceCtrl.provFileOpsList(project, params);
        
      }
      default: {
        ProvFileOpDTO.PList result = provenanceCtrl.provFileOpsList(project, params);
        switch (opsCompaction) {
          case NONE:
            return Response.ok().entity(result).build();
          case FILE_COMPACT:
            List<ProvFileOpsCompactByApp> compactResults = ProvFileOpsCompactByApp.compact(result.getItems());
            return Response.ok().entity(new GenericEntity<List<ProvFileOpsCompactByApp>>(compactResults) {
            }).build();
          case FILE_SUMMARY:
            List<ProvFileOpsSummaryByApp> summaryResults = ProvFileOpsSummaryByApp.summary(result.getItems());
            return Response.ok().entity(new GenericEntity<List<ProvFileOpsSummaryByApp>>(summaryResults) {
            }).build();
          default:
            throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
              "footprint filterType: " + returnType);
        }
      }
    }
  }
}
