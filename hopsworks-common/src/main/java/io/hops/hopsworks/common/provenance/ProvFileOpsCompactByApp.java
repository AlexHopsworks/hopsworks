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
import org.javatuples.Pair;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@XmlRootElement
public class ProvFileOpsCompactByApp {
  private Long inodeId;
  private String inodeName;
  private List<CompactApp> apps = new LinkedList<>();
  
  public ProvFileOpsCompactByApp() {}
  
  private ProvFileOpsCompactByApp(long inodeId, String inodeName) {
    this.inodeId = inodeId;
    this.inodeName = inodeName;
  }
  
  public Long getInodeId() {
    return inodeId;
  }
  
  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }
  
  public String getInodeName() {
    return inodeName;
  }
  
  public void setInodeName(String inodeName) {
    this.inodeName = inodeName;
  }
  
  public List<CompactApp> getApps() {
    return apps;
  }
  
  public void setApps(List<CompactApp> apps) {
    this.apps = apps;
  }
  
  public void addApp(CompactApp app) {
    apps.add(app);
  }
  
  public void sortOpsByTimestamp() {
    for(CompactApp fileOpsByApp : apps) {
      Collections.sort(fileOpsByApp.ops, new Comparator<CompactOpHit>() {
        @Override
        public int compare(CompactOpHit o1, CompactOpHit o2) {
          int compareResult = Longs.compare(o1.timestamp, o2.timestamp);
          if(compareResult == 0) {
            compareResult =  Ints.compare(o1.logicalTime, o2.logicalTime);
          }
          return compareResult;
        }
      });
    }
    Collections.sort(apps, new Comparator<CompactApp>() {
      @Override
      public int compare(CompactApp o1, CompactApp o2) {
        int compareResult = Longs.compare(o1.firstOpTimestamp, o2.firstOpTimestamp);
        if(compareResult == 0) {
          compareResult =  Ints.compare(o1.firstOpLogicalTime, o2.firstOpLogicalTime);
        }
        return compareResult;
      }
    });
  }
  
  public static List<ProvFileOpsCompactByApp> compact(List<FileOp> fileOps) {
    Map<Long, ProvFileOpsCompactByApp> byFileMap = new HashMap<>();
    Map<Pair<Long, String>, CompactApp> byFileAndAppMap = new HashMap<>();
    for(FileOp fileOp : fileOps) {
      ProvFileOpsCompactByApp byFile = byFileMap.get(fileOp.getInodeId());
      if(byFile == null) {
        byFile = new ProvFileOpsCompactByApp(fileOp.getInodeId(), fileOp.getInodeName());
        byFileMap.put(fileOp.getInodeId(), byFile);
      }
      Pair<Long, String> id = Pair.with(fileOp.getInodeId(), fileOp.getAppId());
      CompactApp byFileAndApp = byFileAndAppMap.get(id);
      if(byFileAndApp == null) {
        byFileAndApp = new CompactApp(fileOp.getAppId());
        byFile.addApp(byFileAndApp);
        byFileAndAppMap.put(id, byFileAndApp);
      }
      byFileAndApp.addOp(new CompactOpHit(fileOp.getInodeOperation(), fileOp.getLogicalTime(), fileOp.getTimestamp(),
        fileOp.getReadableTimestamp(), fileOp.getXattrName()));
    }
    List<ProvFileOpsCompactByApp> result = new LinkedList<>();
    for (ProvFileOpsCompactByApp compactOps : byFileMap.values()) {
      compactOps.sortOpsByTimestamp();
      result.add(compactOps);
    }
    return result;
  }
  
  @XmlRootElement
  public static class CompactApp {
    private String appId;
    private Long firstOpTimestamp;
    private Integer firstOpLogicalTime;
    private List<CompactOpHit> ops = new LinkedList<>();
    
    public CompactApp(){}
    
    public CompactApp(String appId) {
      this.appId = appId;
    }
  
    public String getAppId() {
      return appId;
    }
  
    public void setAppId(String appId) {
      this.appId = appId;
    }
  
    public Long getFirstOpTimestamp() {
      return firstOpTimestamp;
    }
  
    public void setFirstOpTimestamp(Long firstOpTimestamp) {
      this.firstOpTimestamp = firstOpTimestamp;
    }
  
    public Integer getFirstOpLogicalTime() {
      return firstOpLogicalTime;
    }
  
    public void setFirstOpLogicalTime(Integer firstOpLogicalTime) {
      this.firstOpLogicalTime = firstOpLogicalTime;
    }
  
    public List<CompactOpHit> getOps() {
      return ops;
    }
  
    public void setOps(List<CompactOpHit> ops) {
      this.ops = ops;
    }
    
    public void addOp(CompactOpHit op) {
      if(ops.isEmpty()) {
        firstOpTimestamp = op.getTimestamp();
        firstOpLogicalTime = op.getLogicalTime();
      }
      if(firstOpTimestamp < op.getTimestamp()) {
        firstOpTimestamp = op.getTimestamp();
      }
      if(firstOpLogicalTime < op.getLogicalTime()) {
        firstOpLogicalTime = op.getLogicalTime();
      }
      ops.add(op);
    }
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
