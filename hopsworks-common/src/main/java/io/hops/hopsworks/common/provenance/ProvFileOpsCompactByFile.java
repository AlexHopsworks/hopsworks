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
package io.hops.hopsworks.common.provenance;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.hops.hopsworks.common.provenance.v2.xml.FileOp;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@XmlRootElement
public class ProvFileOpsCompactByFile {
  private long inodeId;
  private String appId;
  private String inodeName;
  private List<CompactOpHit> ops = new LinkedList<>();
  
  public ProvFileOpsCompactByFile() {}
  
  private ProvFileOpsCompactByFile(long inodeId, String appId, String inodeName) {
    this.inodeId = inodeId;
    this.appId = appId;
    this.inodeName = inodeName;
  }
  
  public long getInodeId() {
    return inodeId;
  }
  
  public void setInodeId(long inodeId) {
    this.inodeId = inodeId;
  }
  
  public String getAppId() {
    return appId;
  }
  
  public void setAppId(String appId) {
    this.appId = appId;
  }
  
  public List<CompactOpHit> getOps() {
    return ops;
  }
  
  public void setOps(List<CompactOpHit> ops) {
    this.ops = ops;
  }
  
  public void addOp(CompactOpHit op) {
    ops.add(op);
  }
  
  public String getInodeName() {
    return inodeName;
  }
  
  public void setInodeName(String inodeName) {
    this.inodeName = inodeName;
  }
  
  public void sortOpsByTimestamp() {
    Collections.sort(ops, new Comparator<CompactOpHit>() {
      @Override
      public int compare(CompactOpHit o1, CompactOpHit o2) {
        int compareResult = 0;
        compareResult = Longs.compare(o1.timestamp, o2.timestamp);
        if(compareResult == 0) {
          compareResult =  Ints.compare(o1.logicalTime, o2.logicalTime);
        }
        return compareResult;
      }
    });
  }
  
  public static List<ProvFileOpsCompactByFile> compact(List<FileOp> fileOps) {
    Map<Long, ProvFileOpsCompactByFile> files = new HashMap<>();
    for(FileOp fileOp : fileOps) {
      ProvFileOpsCompactByFile file = files.get(fileOp.getInodeId());
      if(file == null) {
        file = new ProvFileOpsCompactByFile(fileOp.getInodeId(), fileOp.getAppId(), fileOp.getInodeName());
        files.put(fileOp.getInodeId(), file);
      }
      file.addOp(new CompactOpHit(fileOp.getInodeOperation(), fileOp.getLogicalTime(), fileOp.getTimestamp(),
        fileOp.getReadableTimestamp(), fileOp.getXattrName()));
    }
    List<ProvFileOpsCompactByFile> result = new LinkedList<>();
    for (ProvFileOpsCompactByFile compactOps : files.values()) {
      compactOps.sortOpsByTimestamp();
      result.add(compactOps);
    }
    return result;
  }
  
  @XmlRootElement
  public static class CompactOpHit {
    private String inodeOperation;
    private int logicalTime;
    private long timestamp;
    private String readableTimestamp;
    private String xattrName;
    
    public CompactOpHit() {}
  
    public CompactOpHit(String inodeOperation, int logicalTime, long timestamp, String readableTimestamp,
      String xattrName) {
      this.inodeOperation = inodeOperation;
      this.logicalTime = logicalTime;
      this.timestamp = timestamp;
      this.readableTimestamp = readableTimestamp;
      this.xattrName = xattrName;
    }
  
    public String getInodeOperation() {
      return inodeOperation;
    }
  
    public void setInodeOperation(String inodeOperation) {
      this.inodeOperation = inodeOperation;
    }
  
    public int getLogicalTime() {
      return logicalTime;
    }
  
    public void setLogicalTime(int logicalTime) {
      this.logicalTime = logicalTime;
    }
  
    public long getTimestamp() {
      return timestamp;
    }
  
    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }
  
    public String getReadableTimestamp() {
      return readableTimestamp;
    }
  
    public void setReadableTimestamp(String readableTimestamp) {
      this.readableTimestamp = readableTimestamp;
    }
  
    public String getXattrName() {
      return xattrName;
    }
  
    public void setXattrName(String xattrName) {
      this.xattrName = xattrName;
    }
  }
}
