/*
 * Changes to this file committed after and not including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
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
 *
 * Changes to this file committed before and including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
 * Copyright (C) 2013 - 2018, Logical Clocks AB and RISE SICS AB. All rights reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS  OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package io.hops.hopsworks.api.provenance;

import io.hops.hopsworks.api.filter.Audience;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.project.ProjectController;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.util.CheckedFunction;
import io.hops.hopsworks.common.provenance.v2.xml.SimpleResult;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ProjectException;
import io.hops.hopsworks.jwt.annotation.JWTRequired;
import io.hops.hopsworks.restutils.RESTCodes;
import io.swagger.annotations.Api;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.logging.Level;
import java.util.logging.Logger;

@Stateless
@Api(value = "Global Provenance Service", description = "Global Provenance Service")
@Path("/provenance")
@TransactionAttribute(TransactionAttributeType.NEVER)
public class GlobalProvenanceResource {
  private static final Logger logger = Logger.getLogger(ProjectProvenanceResource.class.getName());
  
  @EJB
  private ProvenanceController provenanceCtrl;
  
  @EJB
  private ProjectController projectCtrl;
  @EJB
  private Settings settings;
  
  private Project getProject(Integer projectId) throws ProjectException {
    return projectCtrl.findProjectById(projectId);
  }
  
  private CheckedFunction<Integer, Project, GenericException> getProjectSupplier() {
    return (Integer projectId) -> {
      try {
        return getProject(projectId);
      } catch (ProjectException e) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "project issue", "exception processing project id filter", e);
      }
    };
  }
  
//  @GET
//  @Path("file/state")
//  @Produces(MediaType.APPLICATION_JSON)
//  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
//  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
//  public Response getFiles(
//    @BeanParam ProvFileStateBeanParam params,
//    @BeanParam Pagination pagination,
//    @Context HttpServletRequest req) throws ServiceException, GenericException {
//    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
//      new Object[]{req.getRequestURL().toString(), params});
//    ProvFileStateParamBuilder paramBuilder = new ProvFileStateParamBuilder()
//      .withQueryParamFileStateFilterBy(params.getFileStateFilterBy())
//      .withQueryParamFileStateSortBy(params.getFileStateSortBy())
//      .withQueryParamExactXAttr(params.getExactXAttrParams())
//      .withQueryParamLikeXAttr(params.getLikeXAttrParams())
//      .withQueryParamXAttrSortBy(params.getXattrSortBy())
//      .withQueryParamExpansions(params.getExpansions())
//      .withQueryParamAppExpansionFilter(params.getAppExpansionParams())
//      .withPagination(pagination.getOffset(), pagination.getLimit());
//    return ProvenanceResourceHelper.getFileStates(provenanceCtrl, paramBuilder, params.getReturnType());
//  }
//
//  @GET
//  @Path("file/ops")
//  @Produces(MediaType.APPLICATION_JSON)
//  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN"})
//  public Response getFileOps(
//    @BeanParam ProvFileOpsBeanParam params,
//    @QueryParam("target_project") Integer projectId,
//    @BeanParam Pagination pagination,
//    @Context HttpServletRequest req) throws ServiceException, GenericException, ProjectException {
//    logger.log(Level.INFO, "Local content path:{0} file ops params:{1} ",
//      new Object[]{req.getRequestURL().toString(), params});
//    ProvFileOpsParamBuilder paramBuilder = new ProvFileOpsParamBuilder()
//      .withQueryParamFilterBy(params.getFileOpsFilterBy())
//      .withQueryParamSortBy(params.getFileOpsSortBy())
//      .withQueryParamExpansions(params.getExpansions())
//      .withQueryParamAppExpansionFilter(params.getAppExpansionParams())
//      .withAggregations(params.getAggregations())
//      .withPagination(pagination.getOffset(), pagination.getLimit());
//    if(projectId != null) {
//      Project project = projectCtrl.findProjectById(projectId);
//      paramBuilder.withProjectInodeId(project.getInode().getId());
//    }
//    return ProvenanceResourceHelper.getFileOps(provenanceCtrl, project, paramBuilder, params.getOpsCompaction(),
//      params.getReturnType());
//  }
//
//  @GET
//  @Path("file/ops/archive")
//  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
//  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
//  @Produces(MediaType.APPLICATION_JSON)
//  public Response getFilesToBeArchived(@BeanParam Pagination pagination)
//    throws ServiceException, GenericException {
//    ProvFileOpsParamBuilder params = new ProvFileOpsParamBuilder()
//      .filterByFileOperation(ProvFileOps.DELETE)
//      .sortByField(ProvElasticFields.FileOpsBase.TIMESTAMP, SortOrder.ASC)
//      .withPagination(0, pagination.getLimit());
//    FileOpDTO.PList result = provenanceCtrl.provFileOpsList(params);
//    return Response.ok().entity(result).build();
//  }
//
//  @GET
//  @Path("project/archive")
//  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
//  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
//  @Produces(MediaType.APPLICATION_JSON)
//  public Response getProjectsToBeArchived(@BeanParam Pagination pagination)
//    throws ServiceException, GenericException {
//    ProvFileOpsParamBuilder params = new ProvFileOpsParamBuilder()
//      .withAggregation(ProvElasticHelper.ProvAggregations.PROJECTS_LEAST_ACTIVE_BY_LAST_ACCESSED)
//      .withPagination(0, 0);
//    FileOpDTO.Count result = provenanceCtrl.provFileOpsCount(params);
//    return Response.ok().entity(result).build();
//  }
//
//  @DELETE
//  @Path("doc/{docId}/cleanup")
//  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
//  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
//  @Produces(MediaType.APPLICATION_JSON)
//  public Response cleanup(
//    @PathParam("docId") String docId,
//    @QueryParam("skipDoc")@DefaultValue("true") boolean skipDoc)
//    throws ServiceException, GenericException {
//    ArchiveDTO.Round result = provenanceCtrl.provCleanupFilePrefix(docId, skipDoc);
//    return Response.ok().entity(result).build();
//  }
//
//  @DELETE
//  @Path("project/cleanup/{limit}")
//  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
//  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
//  @Produces(MediaType.APPLICATION_JSON)
//  public Response cleanup(@PathParam("limit") Integer limit)
//    throws ServiceException, GenericException {
//    ArchiveDTO.Round result = provenanceCtrl.cleanupRound(limit);
//    return Response.ok().entity(result).build();
//  }
//
//  @DELETE
//  @Path("project/archive/{limit}")
//  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
//  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
//  @Produces(MediaType.APPLICATION_JSON)
//  public Response archive(
//    @PathParam("limit") Integer limit)
//    throws ServiceException, GenericException, ProjectException {
//    ArchiveDTO.Round result = provenanceCtrl.archiveRound(limit);
//    return Response.ok().entity(result).build();
//  }
//
//  @DELETE
//  @Path("project/{projectId}/archive/{limit}")
//  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
//  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
//  @Produces(MediaType.APPLICATION_JSON)
//  public Response archive(
//    @PathParam("projectId") Integer projectId,
//    @PathParam("limit") Integer limit)
//    throws ServiceException, GenericException, ProjectException {
//    Project project = projectCtrl.findProjectById(projectId);
//    ArchiveDTO.Round result = provenanceCtrl.projectArchiveRound(project, limit);
//    return Response.ok().entity(result).build();
//  }
//
//  @GET
//  @Path("file/{inodeId}/archive")
//  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
//  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
//  @Produces(MediaType.APPLICATION_JSON)
//  public Response archive(@PathParam("inodeId") Long inodeId)
//    throws ServiceException, GenericException {
//    ArchiveDTO.Base result = provenanceCtrl.getArchiveDoc(inodeId);
//    return Response.ok().entity(result).build();
//  }
  
  
  @GET
  @Path("settings/archive")
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  @Produces(MediaType.APPLICATION_JSON)
  public Response archiveInfo() {
    return Response.ok().entity(new SimpleResult<>(settings.getProvArchiveSize())).build();
  }
  
  @POST
  @Path("settings/archive")
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN"})
  @Produces(MediaType.APPLICATION_JSON)
  public Response archiveInfo(@QueryParam("size") Integer size) {
    if(size != null) {
      settings.setProvArchiveSize(size);
    }
    return Response.ok().build();
  }
}
