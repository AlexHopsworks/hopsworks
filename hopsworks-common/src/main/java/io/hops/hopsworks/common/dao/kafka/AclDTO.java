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

package io.hops.hopsworks.common.dao.kafka;

import io.hops.hopsworks.common.api.RestDTO;

import java.io.Serializable;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class AclDTO extends RestDTO<AclDTO> implements Serializable {

  private Integer id;
  private String projectName;
  private String userEmail;
  private String permissionType;
  private String operationType;
  private String host;
  private String role;

  public AclDTO() {
  }
  
  public AclDTO(Integer id) {
    this.id = id;
  }

  public AclDTO(Integer id, String projectName, String userEmail,
      String permissionType, String operationType, String host, String role) {
    this.id = id;
    this.projectName = projectName;
    this.userEmail = userEmail;
    this.permissionType = permissionType;
    this.operationType = operationType;
    this.host = host;
    this.role = role;
  }
  
  public AclDTO(String projectName, String userEmail,
      String permissionType, String operationType, String host, String role) {
    this.projectName = projectName;
    this.userEmail = userEmail;
    this.permissionType = permissionType;
    this.operationType = operationType;
    this.host = host;
    this.role = role;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  public String getHost() {
    return host;
  }

  public String getOperationType() {
    return operationType;
  }

  public String getPermissionType() {
    return permissionType;
  }

  public String getRole() {
    return role;
  }

  public String getUserEmail() {
    return userEmail;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setOperationType(String operationType) {
    this.operationType = operationType;
  }

  public void setPermissionType(String permissionType) {
    this.permissionType = permissionType;
  }

  public void setRole(String role) {
    this.role = role;
  }

  public void setUserEmail(String userEmail) {
    this.userEmail = userEmail;
  }
}
