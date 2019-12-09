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
package io.hops.hopsworks.common.provenance.xml;

import io.hops.hopsworks.common.provenance.elastic.prov.ProvFileStateElastic;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collection;
import java.util.List;

public class ProvFileStateDTO {
  @XmlRootElement
  public static class PList {
    List<ProvFileStateElastic> items;
    Long count;
    
    public PList() {}
    
    public PList(List<ProvFileStateElastic> items, Long count) {
      this.items = items;
      this.count = count;
    }
    
    public List<ProvFileStateElastic> getItems() {
      return items;
    }
    
    public void setItems(List<ProvFileStateElastic> items) {
      this.items = items;
    }
  
    public Long getCount() {
      return count;
    }
  
    public void setCount(Long count) {
      this.count = count;
    }
  }
  
  @XmlRootElement
  public static class MinTree {
    protected Collection<ProvFileStateTreeDTO> result;
    
    public MinTree() {}
    
    public MinTree(Collection<ProvFileStateTreeDTO> result) {
      this.result = result;
    }
    
    public Collection<ProvFileStateTreeDTO> getResult() {
      return result;
    }
    
    public void setResult(Collection<ProvFileStateTreeDTO> result) {
      this.result = result;
    }
  }
  
  @XmlRootElement
  public static class FullTree {
    protected Collection<ProvFileStateTreeDTO> result;
    protected Collection<ProvFileStateTreeDTO> incomplete;
    
    public FullTree() {}
    
    public FullTree(Collection<ProvFileStateTreeDTO> result, Collection<ProvFileStateTreeDTO> incomplete) {
      this.result = result;
      this.incomplete = incomplete;
    }
    
    public Collection<ProvFileStateTreeDTO> getResult() {
      return result;
    }
    
    public void setResult(Collection<ProvFileStateTreeDTO> result) {
      this.result = result;
    }
    
    public Collection<ProvFileStateTreeDTO> getIncomplete() {
      return incomplete;
    }
    
    public void setIncomplete(Collection<ProvFileStateTreeDTO> incomplete) {
      this.incomplete = incomplete;
    }
  }
}
