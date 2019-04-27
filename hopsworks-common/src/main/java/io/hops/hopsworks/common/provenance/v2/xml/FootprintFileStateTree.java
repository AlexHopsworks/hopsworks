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
package io.hops.hopsworks.common.provenance.v2.xml;

import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

@XmlRootElement
public class FootprintFileStateTree implements ProvenanceController.BasicTreeBuilder<FootprintFileState> {
  private Long inodeId;
  private String name;
  private FootprintFileState fileState;
  private Map<Long, FootprintFileStateTree> children = new HashMap<>();
  
  public FootprintFileStateTree(){}
  
  public FootprintFileStateTree(Long inodeId, String name, FootprintFileState fileState,
    Map<Long, FootprintFileStateTree> children) {
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
  
  public FootprintFileState getFileState() {
    return fileState;
  }
  
  public void setFileState(FootprintFileState fileState) {
    this.fileState = fileState;
  }
  
  public Map<Long, FootprintFileStateTree> getChildren() {
    return children;
  }
  
  public void setChildren(Map<Long, FootprintFileStateTree> children) {
    this.children = children;
  }
  
  @Override
  public void addChild(ProvenanceController.BasicTreeBuilder<FootprintFileState> child) throws GenericException {
    if(child instanceof FootprintFileStateTree) {
      FootprintFileStateTree c = (FootprintFileStateTree) child;
      FootprintFileStateTree aux = children.get(child.getInodeId());
      if(aux != null) {
        TreeHelper.merge(aux, c);
      } else {
        children.put(child.getInodeId(), c);
      }
    } else {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "logic error in FootprintFileStateTree");
    }
  }
}
