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
import io.hops.hopsworks.common.provenance.AppFootprintType;
import io.hops.hopsworks.common.provenance.ProvFileOpsCompactByApp;
import io.hops.hopsworks.common.provenance.ProvFileOpsSummaryByApp;
import io.hops.hopsworks.common.provenance.v2.xml.FileOpDTO;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateDTO;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateTree;
import io.hops.hopsworks.common.provenance.v2.xml.FootprintFileState;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.v2.xml.FootprintFileStateDTO;
import io.hops.hopsworks.common.provenance.v2.xml.SimpleResult;
import io.hops.hopsworks.common.provenance.v2.ProvFileOpsParamBuilder;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.v2.xml.FootprintFileStateTree;
import io.hops.hopsworks.exceptions.GenericException;
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
    throws GenericException, ServiceException {
    try {
      switch (returnType) {
        case LIST:
          FileStateDTO.PList listResult = provenanceCtrl.provFileStateList(project, params);
          return Response.ok().entity(listResult).build();
        case MIN_TREE:
          Pair<Map<Long, FileStateTree>, Map<Long, FileStateTree>> minAux
            = provenanceCtrl.provFileStateTree(project, params, false);
          FileStateDTO.MinTree minTreeResult
            = new FileStateDTO.MinTree(minAux.getValue0().values());
          return Response.ok().entity(minTreeResult).build();
        case FULL_TREE:
          Pair<Map<Long, FileStateTree>, Map<Long, FileStateTree>> fullAux
            = provenanceCtrl.provFileStateTree(project, params, true);
          FileStateDTO.FullTree fullTreeResult
            = new FileStateDTO.FullTree(fullAux.getValue0().values(), fullAux.getValue1().values());
          return Response.ok().entity(fullTreeResult).build();
        case COUNT:
          Long countResult = provenanceCtrl.provFileStateCount(project, params);
          return Response.ok().entity(new SimpleResult<>(countResult)).build();
        default:
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
            "return type: " + returnType + " is not managed");
      }
    } catch (GenericException | ServiceException e) {
      throw e;
    } catch(Exception e) {
      LOG.log(Level.WARNING, "prov exception: ", e);
      throw e;
    }
  }
  
  public static Response getAppFootprint(ProvenanceController provenanceCtrl, Project project,
    ProvFileOpsParamBuilder params, AppFootprintType footprintType,
    ProjectProvenanceResource.FileStructReturnType returnType)
    throws GenericException, ServiceException {
    try {
      switch(returnType) {
        case LIST:
          List<FootprintFileState> listAux = provenanceCtrl.provAppFootprintList(project, params, footprintType);
          FootprintFileStateDTO.PList listResult = new FootprintFileStateDTO.PList(listAux);
          return Response.ok().entity(listResult).build();
        case MIN_TREE:
          Pair<Map<Long, FootprintFileStateTree>, Map<Long, FootprintFileStateTree>> minAux
            = provenanceCtrl.provAppFootprintTree(project, params, footprintType, false);
          FootprintFileStateDTO.MinTree minTreeResult
            = new FootprintFileStateDTO.MinTree(minAux.getValue0().values());
          return Response.ok().entity(minTreeResult).build();
        case FULL_TREE:
          Pair<Map<Long, FootprintFileStateTree>, Map<Long, FootprintFileStateTree>> fullAux
            = provenanceCtrl.provAppFootprintTree(project, params, footprintType,true);
          FootprintFileStateDTO.FullTree fullTreeResult
            = new FootprintFileStateDTO.FullTree(fullAux.getValue0().values(), fullAux.getValue1().values());
          return Response.ok().entity(fullTreeResult).build();
        case ARTIFACTS:
          FileOpDTO.Count aux = provenanceCtrl.provAppArtifactFootprint(project, params);
          return Response.ok().entity(aux).build();
        case COUNT:
        default:
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
            "return type: " + returnType + " is not managed");
      }
    } catch (GenericException | ServiceException e) {
      throw e;
    } catch(Exception e) {
      LOG.log(Level.WARNING, "prov exception: ", e);
      throw e;
    }
  }
  
  
  public static Response getFileOps(ProvenanceController provenanceCtrl, Project project,
    ProvFileOpsParamBuilder params, ProjectProvenanceResource.FileOpsCompactionType opsCompaction,
    ProjectProvenanceResource.FileStructReturnType returnType)
    throws ServiceException, GenericException {
    try {
      switch(returnType) {
        case COUNT: {
          FileOpDTO.Count result = provenanceCtrl.provFileOpsCount(project, params);
          return Response.ok().entity(result).build();
        }
        case ARTIFACTS: {
          FileOpDTO.PList result = provenanceCtrl.provFileOpsList(project, params);
          
        }
        default: {
          FileOpDTO.PList result = provenanceCtrl.provFileOpsList(project, params);
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
              throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
                "footprint filterType: " + returnType);
          }
        }
      }
    } catch (GenericException | ServiceException e) {
      throw e;
    } catch(Exception e) {
      LOG.log(Level.WARNING, "prov exception: ", e);
      throw e;
    }
  }
}
