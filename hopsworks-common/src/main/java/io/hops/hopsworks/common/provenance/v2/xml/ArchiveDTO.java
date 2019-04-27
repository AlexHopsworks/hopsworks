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

import io.hops.hopsworks.common.provenance.v2.ProvElasticFields;
import io.hops.hopsworks.common.provenance.v2.ProvHelper;
import io.hops.hopsworks.exceptions.GenericException;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.Map;


public class ArchiveDTO {
  @XmlRootElement
  public static class Base {
    private Long inodeId;
    private String archived;
  
    public Base() {
    }
  
    public static Base instance(Map<String, Object> sourceMap) throws GenericException {
      ArchiveDTO.Base result = new ArchiveDTO.Base();
      Map<String, Object> auxMap = new HashMap<>(sourceMap);
      result.inodeId = ProvElasticFields.extractField(auxMap,
        ProvElasticFields.FileBase.INODE_ID, ProvHelper.asLong(false));
      result.archived = ProvElasticFields.extractField(auxMap,
        ProvElasticFields.FileBase.INODE_ID, ProvHelper.asString(true));
      return result;
    }
    
    public Long getInodeId() {
      return inodeId;
    }
  
    public void setInodeId(Long inodeId) {
      this.inodeId = inodeId;
    }
  
    public String getArchived() {
      return archived;
    }
  
    public void setArchived(String archived) {
      this.archived = archived;
    }
  }
  
  @XmlRootElement
  public static class Round {
    private Long archived;
    private Long cleaned;
  
    public Round() {
    }
  
    public Round(Long archived, Long cleaned) {
      this.archived = archived;
      this.cleaned = cleaned;
    }
  
    public Long getArchived() {
      return archived;
    }
  
    public void setArchived(Long archived) {
      this.archived = archived;
    }
  
    public Long getCleaned() {
      return cleaned;
    }
  
    public void setCleaned(Long cleaned) {
      this.cleaned = cleaned;
    }
  }
}
