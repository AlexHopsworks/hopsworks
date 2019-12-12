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
package io.hops.hopsworks.api.provenance;

import io.hops.hopsworks.api.filter.AllowedProjectRoles;
import io.hops.hopsworks.api.filter.Audience;
import io.hops.hopsworks.api.jwt.JWTHelper;
import io.hops.hopsworks.api.provenance.ops.ProvFileOpsBeanParam;
import io.hops.hopsworks.api.provenance.state.ProvFileStateBeanParam;
import io.hops.hopsworks.api.util.Pagination;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.dao.user.Users;
import io.hops.hopsworks.common.provenance.core.HopsFSProvenanceController;
import io.hops.hopsworks.common.provenance.core.dto.ProvDatasetDTO;
import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.ops.ProvArchivalController;
import io.hops.hopsworks.common.provenance.ops.ProvFileOpsParamBuilder;
import io.hops.hopsworks.common.provenance.ops.ProvOpsController;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpDTO;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpsCompactByApp;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFileOpsSummaryByApp;
import io.hops.hopsworks.common.provenance.ops.dto.ProvStateMinDTO;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFootprintFileStateTreeElastic;
import io.hops.hopsworks.common.provenance.ops.dto.ProvFootprintFileStatesElastic;
import io.hops.hopsworks.common.provenance.state.ProvStateController;
import io.hops.hopsworks.common.provenance.core.dto.ProvTypeDTO;
import io.hops.hopsworks.common.provenance.state.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.state.dto.ProvStateListDTO;
import io.hops.hopsworks.common.provenance.util.ProvHelper;
import io.hops.hopsworks.common.provenance.util.dto.WrapperDTO;
import io.hops.hopsworks.exceptions.ElasticException;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.jwt.annotation.JWTRequired;
import io.hops.hopsworks.restutils.RESTCodes;
import io.swagger.annotations.Api;
import org.javatuples.Pair;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.context.RequestScoped;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BeanParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

@RequestScoped
@TransactionAttribute(TransactionAttributeType.NEVER)
@Api(value = "Provenance Service", description = "Provenance Service")
public class ProvenanceResource {
  private static final Logger logger = Logger.getLogger(ProvenanceResource.class.getName());
  
  @EJB
  private ProjectFacade projectFacade;
  @EJB
  private JWTHelper jWTHelper;
  @EJB
  private ProvStateController stateProvCtrl;
  @EJB
  private ProvOpsController opsProvCtrl;
  @EJB
  private ProvArchivalController archiveCtrl;
  @EJB
  private HopsFSProvenanceController fsProvenanceCtrl;
  
  private Project project;
  
  public void setProjectId(Integer projectId) {
    this.project = projectFacade.find(projectId);
  }
  
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.ANYONE})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getProvenanceStatus(
    @QueryParam("type") @DefaultValue("PROJECT") TypeOf typeOf,
    @Context SecurityContext sc)
    throws ProvenanceException {
    Users user = jWTHelper.getUserPrincipal(sc);
    switch(typeOf) {
      case PROJECT:
        ProvTypeDTO status = fsProvenanceCtrl.getProjectProvType(user, project);
        return Response.ok().entity(status).build();
      case DATASETS:
        GenericEntity<List<ProvDatasetDTO>> result
          = new GenericEntity<List<ProvDatasetDTO>>(fsProvenanceCtrl.getDatasetsProvType(user, project)) {};
        return Response.ok().entity(result).build();
      default:
        throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
          "return type: " + typeOf + " is not managed");
    }
  }
  
  public enum TypeOf {
    PROJECT,
    DATASETS
  }
  
  @GET
  @Path("/states")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getFileStates(
    @BeanParam ProvFileStateBeanParam params,
    @BeanParam Pagination pagination,
    @Context HttpServletRequest req) throws ProvenanceException {
    ProvFileStateParamBuilder paramBuilder = new ProvFileStateParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withQueryParamFileStateFilterBy(params.getFileStateFilterBy())
      .withQueryParamFileStateSortBy(params.getFileStateSortBy())
      .withQueryParamExactXAttr(params.getExactXAttrParams())
      .withQueryParamLikeXAttr(params.getLikeXAttrParams())
      .filterByHasXAttr(params.getFilterByHasXAttrs())
      .withQueryParamXAttrSortBy(params.getXattrSortBy())
      .withQueryParamExpansions(params.getExpansions())
      .withQueryParamAppExpansionFilter(params.getAppExpansionParams())
      .withPagination(pagination.getOffset(), pagination.getLimit());
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    return getFileStates(project, paramBuilder, params.getReturnType());
  }
  
  private Response getFileStates(Project project,
    ProvFileStateParamBuilder params, FileStructReturnType returnType)
    throws ProvenanceException {
    switch (returnType) {
      case LIST:
        ProvStateListDTO listResult = stateProvCtrl.provFileStateList(project, params);
        return Response.ok().entity(listResult).build();
      case COUNT:
        Long countResult = stateProvCtrl.provFileStateCount(project, params);
        return Response.ok().entity(new WrapperDTO<>(countResult)).build();
      default:
        throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
          "return type: " + returnType + " is not managed");
    }
  }
  
  public enum FileStructReturnType {
    LIST,
    COUNT,
    MIN_TREE,
    FULL_TREE,
    ARTIFACTS;
  }
  
  public enum FileOpsCompactionType {
    NONE,
    FILE_COMPACT,
    FILE_SUMMARY
  }
  
  //ops
  @GET
  @Path("ops")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getFileOps(
    @BeanParam ProvFileOpsBeanParam params,
    @BeanParam Pagination pagination,
    @QueryParam("cleanup") @DefaultValue("false") boolean cleanup,
    @Context HttpServletRequest req) throws ProvenanceException {
    ProvFileOpsParamBuilder paramBuilder = new ProvFileOpsParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withQueryParamFilterBy(params.getFileOpsFilterBy())
      .withQueryParamSortBy(params.getFileOpsSortBy())
      .withQueryParamExpansions(params.getExpansions())
      .withQueryParamAppExpansionFilter(params.getAppExpansionParams())
      .withAggregations(params.getAggregations())
      .withPagination(pagination.getOffset(), pagination.getLimit());
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    if(cleanup) {
      ProvFileOpDTO.Count result;
      try {
        result = archiveCtrl.cleanupSize(project);
      } catch (ElasticException e) {
        String msg = "provenance - elastic query problem";
        throw ProvHelper.fromElastic(e, msg, msg + " - file ops");
      }
      return Response.ok().entity(result).build();
    } else {
      return getFileOps(project, paramBuilder, params.getOpsCompaction(), params.getReturnType());
    }
  }
  
  @GET
  @Path("ops/{inodeId}")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getFileOps(
    @PathParam("inodeId") Long fileInodeId,
    @BeanParam ProvFileOpsBeanParam params,
    @BeanParam Pagination pagination,
    @Context HttpServletRequest req) throws ProvenanceException {
    ProvFileOpsParamBuilder paramBuilder = new ProvFileOpsParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withFileInodeId(fileInodeId)
      .withQueryParamFilterBy(params.getFileOpsFilterBy())
      .withQueryParamSortBy(params.getFileOpsSortBy())
      .withQueryParamExpansions(params.getExpansions())
      .withQueryParamAppExpansionFilter(params.getAppExpansionParams())
      .withAggregations(params.getAggregations())
      .withPagination(pagination.getOffset(), pagination.getLimit());
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    return getFileOps(project, paramBuilder, params.getOpsCompaction(), params.getReturnType());
  }
  
  @GET
  @Path("footprint/{type}/app/{appId}/")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response appProvenance(
    @PathParam("appId") String appId,
    @PathParam("type") @DefaultValue("ALL")
      Provenance.FootprintType footprintType,
    @BeanParam ProvFileOpsBeanParam params,
    @BeanParam Pagination pagination,
    @Context HttpServletRequest req) throws ProvenanceException {
    ProvFileOpsParamBuilder paramBuilder = new ProvFileOpsParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withAppId(appId)
      .withQueryParamFilterBy(params.getFileOpsFilterBy())
      .withQueryParamSortBy(params.getFileOpsSortBy())
      .withAggregations(params.getAggregations())
      .withPagination(pagination.getOffset(), pagination.getLimit());
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    
    return getAppFootprint(project, paramBuilder, footprintType, params.getReturnType());
  }
  
  private Response getAppFootprint(Project project,
    ProvFileOpsParamBuilder params, Provenance.FootprintType footprintType,
    ProvenanceResource.FileStructReturnType returnType)
    throws ProvenanceException {
    switch(returnType) {
      case LIST:
        List<ProvStateMinDTO> listAux
          = opsProvCtrl.provAppFootprintList(project, params, footprintType);
        ProvFootprintFileStatesElastic.PList listResult = new ProvFootprintFileStatesElastic.PList(listAux);
        return Response.ok().entity(listResult).build();
      case MIN_TREE:
        Pair<Map<Long, ProvFootprintFileStateTreeElastic>, Map<Long, ProvFootprintFileStateTreeElastic>> minAux
          = opsProvCtrl.provAppFootprintTree(project, params, footprintType, false);
        ProvFootprintFileStatesElastic.MinTree minTreeResult
          = new ProvFootprintFileStatesElastic.MinTree(minAux.getValue0().values());
        return Response.ok().entity(minTreeResult).build();
      case FULL_TREE:
        Pair<Map<Long, ProvFootprintFileStateTreeElastic>, Map<Long, ProvFootprintFileStateTreeElastic>> fullAux
          = opsProvCtrl.provAppFootprintTree(project, params, footprintType,true);
        ProvFootprintFileStatesElastic.FullTree fullTreeResult
          = new ProvFootprintFileStatesElastic.FullTree(fullAux.getValue0().values(), fullAux.getValue1().values());
        return Response.ok().entity(fullTreeResult).build();
      case ARTIFACTS:
        ProvFileOpDTO.Count aux = opsProvCtrl.provAppArtifactFootprint(project, params);
        return Response.ok().entity(aux).build();
      case COUNT:
      default:
        throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.UNSUPPORTED, Level.INFO,
          "return type: " + returnType + " is not managed");
    }
  }
  
  private Response getFileOps(Project project,
    ProvFileOpsParamBuilder params, ProvenanceResource.FileOpsCompactionType opsCompaction,
    ProvenanceResource.FileStructReturnType returnType)
    throws ProvenanceException {
    switch(returnType) {
      case COUNT: {
        ProvFileOpDTO.Count result = opsProvCtrl.provFileOpsCount(project, params);
        return Response.ok().entity(result).build();
      }
      case ARTIFACTS: {
        ProvFileOpDTO.PList result = opsProvCtrl.provFileOpsList(project, params);
        return Response.ok().entity(result).build();
      }
      default: {
        ProvFileOpDTO.PList result = opsProvCtrl.provFileOpsList(project, params);
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
