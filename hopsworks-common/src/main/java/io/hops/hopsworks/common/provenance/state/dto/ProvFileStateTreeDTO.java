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
package io.hops.hopsworks.common.provenance.state.dto;

import io.hops.hopsworks.common.provenance.state.ProvTree;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.restutils.RESTCodes;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

@XmlRootElement
public class ProvFileStateTreeDTO implements ProvTree.Builder<ProvFileStateElastic> {
  private Long inodeId;
  private String name;
  private ProvFileStateElastic fileState;
  private Map<Long, ProvFileStateTreeDTO> children = new HashMap<>();
  
  public ProvFileStateTreeDTO(){}
  
  public ProvFileStateTreeDTO(Long inodeId, String name, ProvFileStateElastic fileState,
    Map<Long, ProvFileStateTreeDTO> children) {
    this.inodeId = inodeId;
    this.name = name;
    this.fileState = fileState;
    this.children = children;
  }
  
  @Override
  public Long getInodeId() {
    return inodeId;
  }
  
  @Override
  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }
  
  @Override
  public String getName() {
    return name;
  }
  
  @Override
  public void setName(String name) {
    this.name = name;
  }
  
  @Override
  public ProvFileStateElastic getFileState() {
    return fileState;
  }
  
  @Override
  public void setFileState(ProvFileStateElastic fileState) {
    this.fileState = fileState;
  }
  
  public Map<Long, ProvFileStateTreeDTO> getChildren() {
    return children;
  }
  
  public void setChildren(Map<Long, ProvFileStateTreeDTO> children) {
    this.children = children;
  }
  
  @Override
  public void addChild(ProvTree.Builder<ProvFileStateElastic> child)
    throws ProvenanceException {
    if(child instanceof ProvFileStateTreeDTO) {
      ProvFileStateTreeDTO c = (ProvFileStateTreeDTO) child;
      ProvFileStateTreeDTO aux = children.get(child.getInodeId());
      if(aux != null) {
        ProvTree.merge(aux, c);
      } else {
        children.put(child.getInodeId(), c);
      }
    } else {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.INTERNAL_ERROR, Level.INFO,
        "logic error in FileStateTree");
    }
  }
}
