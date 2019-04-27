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

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collection;
import java.util.List;

public class FootprintFileStateDTO {
  @XmlRootElement
  public static class PList {
    List<FootprintFileState> items;
    
    public PList() {}
    
    public PList(List<FootprintFileState> items) {
      this.items = items;
    }
    
    public List<FootprintFileState> getItems() {
      return items;
    }
    
    public void setItems(List<FootprintFileState> items) {
      this.items = items;
    }
  }
  
  @XmlRootElement
  public static class MinTree {
    protected Collection<FootprintFileStateTree> items;
    
    public MinTree() {}
    
    public MinTree(Collection<FootprintFileStateTree> items) {
      this.items = items;
    }
    
    public Collection<FootprintFileStateTree> getItems() {
      return items;
    }
    
    public void setItems(Collection<FootprintFileStateTree> items) {
      this.items = items;
    }
  }
  
  @XmlRootElement
  public static class FullTree {
    protected Collection<FootprintFileStateTree> items;
    protected Collection<FootprintFileStateTree> incomplete;
    
    public FullTree() {}
    
    public FullTree(Collection<FootprintFileStateTree> items, Collection<FootprintFileStateTree> incomplete) {
      this.items = items;
      this.incomplete = incomplete;
    }
    
    public Collection<FootprintFileStateTree> getItems() {
      return items;
    }
    
    public void setItems(Collection<FootprintFileStateTree> items) {
      this.items = items;
    }
    
    public Collection<FootprintFileStateTree> getIncomplete() {
      return incomplete;
    }
    
    public void setIncomplete(Collection<FootprintFileStateTree> incomplete) {
      this.incomplete = incomplete;
    }
  }
}
