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
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.restutils.RESTCodes;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

@XmlRootElement
public class ProvFootprintFileStateTreeElastic implements ProvTree.Builder<ProvStateMinDTO> {
  private Long inodeId;
  private String name;
  private ProvStateMinDTO fileState;
  private Map<Long, ProvFootprintFileStateTreeElastic> children = new HashMap<>();
  
  public ProvFootprintFileStateTreeElastic(){}
  
  public ProvFootprintFileStateTreeElastic(Long inodeId, String name, ProvStateMinDTO fileState,
    Map<Long, ProvFootprintFileStateTreeElastic> children) {
    this.inodeId = inodeId;
    this.name = name;
    this.fileState = fileState;
    this.children = children;
  }
  
  public Long getInodeId() {
    return inodeId;
  }
  
  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }
  
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public ProvStateMinDTO getFileState() {
    return fileState;
  }
  
  public void setFileState(ProvStateMinDTO fileState) {
    this.fileState = fileState;
  }
  
  public Map<Long, ProvFootprintFileStateTreeElastic> getChildren() {
    return children;
  }
  
  public void setChildren(Map<Long, ProvFootprintFileStateTreeElastic> children) {
    this.children = children;
  }
  
  @Override
  public void addChild(ProvTree.Builder<ProvStateMinDTO> child)
    throws ProvenanceException {
    if(child instanceof ProvFootprintFileStateTreeElastic) {
      ProvFootprintFileStateTreeElastic c = (ProvFootprintFileStateTreeElastic) child;
      ProvFootprintFileStateTreeElastic aux = children.get(child.getInodeId());
      if(aux != null) {
        ProvTree.merge(aux, c);
      } else {
        children.put(child.getInodeId(), c);
      }
    } else {
      throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.INTERNAL_ERROR, Level.INFO,
        "logic error in FootprintFileStateTree");
    }
  }
}
