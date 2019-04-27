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
package io.hops.hopsworks.common.provenance.core;

import io.hops.hopsworks.common.elastic.BasicElasticHit;
import io.hops.hopsworks.common.elastic.ElasticHitsHandler;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvFileOpsParamBuilder;
import io.hops.hopsworks.common.provenance.apiToElastic.ProvRestToElastic;
import io.hops.hopsworks.common.provenance.elastic.ProvFileOpElastic;
import io.hops.hopsworks.common.provenance.xml.ProvFileStateMinDTO;
import io.hops.hopsworks.common.provenance.xml.ProvMLStateMinDTO;
import io.hops.hopsworks.exceptions.ProvenanceException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProvExecution {
  public static class Footprint<K,V extends FootprintItem> {
    Map<K, V> state = new HashMap<>();
    Set<K> accessed = new HashSet<>();
    Set<K> created = new HashSet<>();
    Set<K> modified = new HashSet<>();
    Set<K> deleted = new HashSet<>();
  }
  
  public interface FootprintItem<K> {
    public K getKey();
    public void setFootprintType(Provenance.FootprintType type);
  }
  
  public static ElasticHitsHandler<ProvFileOpElastic, Footprint<String, ProvMLStateMinDTO>, ?, ProvenanceException>
    mlStateProc() {
    return ElasticHitsHandler.instanceBasic(new ProvExecution.Footprint<>(),
      (BasicElasticHit hit) -> ProvFileOpElastic.instance(hit, false),
      (ProvFileOpElastic op, Footprint<String, ProvMLStateMinDTO> state) -> {
        Provenance.MLType mlType = ProvRestToElastic.mlTypeParser(op.getDocSubType());
        ProvMLStateMinDTO mlState = ProvMLStateMinDTO.fromFileOp(op, mlType);
        addToFootprint(state, mlState, op);
      });
  }
  
  public static Map<Long, ProvFileStateMinDTO> processFootprint(List<ProvFileOpElastic> fileOps,
    Provenance.FootprintType footprintType) {
    Footprint<Long, ProvFileStateMinDTO> rawFootprint = new Footprint<>();
    for(ProvFileOpElastic fileOp : fileOps) {
      Provenance.MLType mlType = ProvRestToElastic.mlTypeParser(fileOp.getDocSubType());
      ProvFileStateMinDTO fileState = ProvFileStateMinDTO.fromFileOp(fileOp, mlType);
      addToFootprint(rawFootprint, fileState, fileOp);
    }
    Map<Long, ProvFileStateMinDTO> footprint = processFootprint(rawFootprint, footprintType);
    return footprint;
  }
  
  public static <K,V extends FootprintItem<K>> Map<K,V> processFootprint(Footprint<K, V> footprint,
    Provenance.FootprintType type) {
    Map<K,V> result = new HashMap<>(footprint.state);
    switch(type) {
      case ALL:
        Set<K> input = footprintInput(footprint.accessed, footprint.created);
        Set<K> outputM = footprintOutputModified(footprint.modified, footprint.created, footprint.deleted);
        Set<K> outputA = footprintOutputAdded(footprint.created, footprint.deleted);
        Set<K> outputR = footprintOutputRemoved(footprint.created, footprint.deleted);
        Set<K> outputT = footprintOutputTmp(footprint.created, footprint.deleted);
        result.entrySet().forEach(entry -> {
          if(input.contains(entry.getKey())) {
            entry.getValue().setFootprintType(Provenance.FootprintType.INPUT);
          } else if(outputM.contains(entry.getKey())) {
            entry.getValue().setFootprintType(Provenance.FootprintType.OUTPUT_MODIFIED);
          } else if(outputA.contains(entry.getKey())) {
            entry.getValue().setFootprintType(Provenance.FootprintType.OUTPUT_ADDED);
          } else if(outputR.contains(entry.getKey())) {
            entry.getValue().setFootprintType(Provenance.FootprintType.OUTPUT_REMOVED);
          } else if(outputT.contains(entry.getKey())) {
            entry.getValue().setFootprintType(Provenance.FootprintType.OUTPUT_TMP);
          }
        });
        break;
      case INPUT: {
        Set<K> aux = footprintInput(footprint.accessed, footprint.created);
        result.keySet().removeIf(id -> !aux.contains(id));
        result.values().forEach(state -> state.setFootprintType(Provenance.FootprintType.INPUT));
      } break;
      case OUTPUT_MODIFIED: {
        Set<K> aux = footprintOutputModified(footprint.modified, footprint.created, footprint.deleted);
        result.keySet().removeIf(id -> !aux.contains(id));
        result.values().forEach(state -> state.setFootprintType(Provenance.FootprintType.OUTPUT_MODIFIED));
      } break;
      case OUTPUT_ADDED: {
        Set<K> aux = footprintOutputAdded(footprint.created, footprint.deleted);
        result.keySet().removeIf(id -> !aux.contains(id));
        result.values().forEach(state -> state.setFootprintType(Provenance.FootprintType.OUTPUT_ADDED));
      } break;
      case OUTPUT_REMOVED: {
        Set<K> aux = footprintOutputRemoved(footprint.created, footprint.deleted);
        result.keySet().removeIf(id -> !aux.contains(id));
        result.values().forEach(state -> state.setFootprintType(Provenance.FootprintType.OUTPUT_TMP));
      } break;
      case OUTPUT_TMP: {
        Set<K> aux = footprintOutputTmp(footprint.created, footprint.deleted);
        result.keySet().removeIf(id -> !aux.contains(id));
        result.values().forEach(state -> state.setFootprintType(Provenance.FootprintType.OUTPUT_TMP));
      } break;
      default:
        //continue;
    }
    return result;
  }
  
  /**
   * artifact read - that existed before execution (not created by execution)
   */
  private static <K> Set<K> footprintInput(Set<K> accessed, Set<K> created) {
    Set<K> result = new HashSet<>(accessed);
    result.removeAll(created);
    return result;
  }
  
  /**
   * artifacts modified, but not created/deleted in this execution
   */
  private static <K> Set<K> footprintOutputModified(Set<K> modified, Set<K> created, Set<K> deleted) {
    Set<K> result = new HashSet<>(modified);
    result.removeAll(created);
    result.removeAll(deleted);
    return result;
  }
  
  /**
   * artifacts created, but not deleted in this execution
   */
  private static <K> Set<K> footprintOutputAdded(Set<K> created, Set<K> deleted) {
    Set<K> result = new HashSet<>(created);
    result.removeAll(deleted);
    return result;
  }
  
  /**
   * artifacts deleted, but not created in this execution
   */
  private static <K> Set<K> footprintOutputRemoved(Set<K> created, Set<K> deleted) {
    Set<K> result = new HashSet<>(deleted);
    result.removeAll(created);
    return result;
  }
  
  /**
   * artifacts tmp, created and deleted
   */
  private static <K> Set<K> footprintOutputTmp(Set<K> created, Set<K> deleted) {
    Set<K> result = new HashSet<>(created);
    result.retainAll(deleted);
    return result;
  }
  
  private static <K,V extends FootprintItem<K>> void addToFootprint(Footprint<K, V> footprint,
    V state, ProvFileOpElastic fileOp) {
    if (!footprint.state.containsKey(state.getKey())) {
      footprint.state.put(state.getKey(), state);
    }
    switch(fileOp.getInodeOperation()) {
      case CREATE:
        footprint.created.add(state.getKey());
        break;
      case DELETE:
        footprint.deleted.add(state.getKey());
        break;
      case ACCESS_DATA:
        footprint.accessed.add(state.getKey());
        break;
      case MODIFY_DATA:
        footprint.modified.add(state.getKey());
        break;
      default:
    }
  }
  
  private void processMLFootprint(Footprint<String, ProvMLStateMinDTO> footprint, ProvFileOpElastic fileOp) {
    if(fileOp.getDocSubType().isMLParent() || fileOp.getDocSubType().isMLPart()) {
      ProvMLStateMinDTO mlA;
      switch(fileOp.getDocSubType()) {
        case FEATURE:
          mlA = ProvMLStateMinDTO.fromFileOp(fileOp, Provenance.MLType.FEATURE);
          break;
        case TRAINING_DATASET:
          mlA = ProvMLStateMinDTO.fromFileOp(fileOp, Provenance.MLType.TRAINING_DATASET);
          break;
        case EXPERIMENT:
          mlA = ProvMLStateMinDTO.fromFileOp(fileOp, Provenance.MLType.EXPERIMENT);
          break;
        case MODEL:
          mlA = ProvMLStateMinDTO.fromFileOp(fileOp, Provenance.MLType.MODEL);
          break;
        default:
          return;
      }
      footprint.state.put(mlA.getMlId(), mlA);
    } else {
      return;
    }
    switch(fileOp.getInodeOperation()) {
      case CREATE:
        if(fileOp.getDocSubType().isMLPart()) {
          footprint.modified.add(fileOp.getMlId());
        } else {
          footprint.created.add(fileOp.getMlId());
        }
        break;
      case DELETE:
        if(fileOp.getDocSubType().isMLPart()) {
          footprint.modified.add(fileOp.getMlId());
        } else {
          footprint.deleted.add(fileOp.getMlId());
        }
        break;
      case ACCESS_DATA:
        footprint.accessed.add(fileOp.getMlId());
        break;
      case MODIFY_DATA:
        footprint.modified.add(fileOp.getMlId());
        break;
      default:
    }
  }
  
  public static void addFootprintQueryParams(ProvFileOpsParamBuilder params, Provenance.FootprintType footprintType)
    throws ProvenanceException {
    switch(footprintType) {
      case ALL:
        break;
      case INPUT:
        params
          .filterByFileOperation(Provenance.FileOps.CREATE)
          .filterByFileOperation(Provenance.FileOps.ACCESS_DATA);
        break;
      case OUTPUT_MODIFIED:
        params
          .filterByFileOperation(Provenance.FileOps.CREATE)
          .filterByFileOperation(Provenance.FileOps.MODIFY_DATA)
          .filterByFileOperation(Provenance.FileOps.DELETE);
        break;
      case OUTPUT_ADDED:
      case OUTPUT_TMP:
      case OUTPUT_REMOVED:
        params
          .filterByFileOperation(Provenance.FileOps.CREATE)
          .filterByFileOperation(Provenance.FileOps.DELETE);
        break;
      default:
        throw new IllegalArgumentException("footprint filterType:" + footprintType + " not managed");
    }
  }
}
