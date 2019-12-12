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
package io.hops.hopsworks.common.provenance.ops.dto;

import io.hops.hopsworks.common.provenance.state.ProvTree;
import io.hops.hopsworks.common.provenance.state.dto.ProvStateElastic;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.restutils.RESTCodes;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

@XmlRootElement
public class ProvStateTreeDTO implements ProvTree.Builder<ProvStateElastic> {
  private Long inodeId;
  private String name;
  private ProvStateElastic fileState;
  private Map<Long, ProvStateTreeDTO> children = new HashMap<>();
  
  public ProvStateTreeDTO(){}
  
  public ProvStateTreeDTO(Long inodeId, String name, ProvStateElastic fileState,
    Map<Long, ProvStateTreeDTO> children) {
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
  public ProvStateElastic getFileState() {
    return fileState;
  }
  
  @Override
  public void setFileState(ProvStateElastic fileState) {
    this.fileState = fileState;
  }
  
  public Map<Long, ProvStateTreeDTO> getChildren() {
    return children;
  }
  
  public void setChildren(Map<Long, ProvStateTreeDTO> children) {
    this.children = children;
  }
  
  @Override
  public void addChild(ProvTree.Builder<ProvStateElastic> child)
    throws ProvenanceException {
    if(child instanceof ProvStateTreeDTO) {
      ProvStateTreeDTO c = (ProvStateTreeDTO) child;
      ProvStateTreeDTO aux = children.get(child.getInodeId());
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
