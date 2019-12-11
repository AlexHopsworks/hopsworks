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

import io.hops.hopsworks.common.provenance.state.dto.ProvFileStateMinDTO;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collection;
import java.util.List;

public class ProvFootprintFileStatesElastic {
  @XmlRootElement
  public static class PList {
    List<ProvFileStateMinDTO> items;
    
    public PList() {}
    
    public PList(List<ProvFileStateMinDTO> items) {
      this.items = items;
    }
    
    public List<ProvFileStateMinDTO> getItems() {
      return items;
    }
    
    public void setItems(List<ProvFileStateMinDTO> items) {
      this.items = items;
    }
  }
  
  @XmlRootElement
  public static class MinTree {
    protected Collection<ProvFootprintFileStateTreeElastic> items;
    
    public MinTree() {}
    
    public MinTree(Collection<ProvFootprintFileStateTreeElastic> items) {
      this.items = items;
    }
    
    public Collection<ProvFootprintFileStateTreeElastic> getItems() {
      return items;
    }
    
    public void setItems(Collection<ProvFootprintFileStateTreeElastic> items) {
      this.items = items;
    }
  }
  
  @XmlRootElement
  public static class FullTree {
    protected Collection<ProvFootprintFileStateTreeElastic> items;
    protected Collection<ProvFootprintFileStateTreeElastic> incomplete;
    
    public FullTree() {}
    
    public FullTree(Collection<ProvFootprintFileStateTreeElastic> items,
      Collection<ProvFootprintFileStateTreeElastic> incomplete) {
      this.items = items;
      this.incomplete = incomplete;
    }
    
    public Collection<ProvFootprintFileStateTreeElastic> getItems() {
      return items;
    }
    
    public void setItems(Collection<ProvFootprintFileStateTreeElastic> items) {
      this.items = items;
    }
    
    public Collection<ProvFootprintFileStateTreeElastic> getIncomplete() {
      return incomplete;
    }
    
    public void setIncomplete(Collection<ProvFootprintFileStateTreeElastic> incomplete) {
      this.incomplete = incomplete;
    }
  }
}
