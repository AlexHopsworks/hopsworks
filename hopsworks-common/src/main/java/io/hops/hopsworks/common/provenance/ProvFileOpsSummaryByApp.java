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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@XmlRootElement
public class ProvFileOpsSummaryByApp {
  private long inodeId;
  private String inodeName;
  private List<CompactApp> apps = new LinkedList<>();
  
  public ProvFileOpsSummaryByApp() {}
  
  private ProvFileOpsSummaryByApp(long inodeId, String inodeName) {
    this.inodeId = inodeId;
    this.inodeName = inodeName;
  }
  
  public long getInodeId() {
    return inodeId;
  }
  
  public void setInodeId(long inodeId) {
    this.inodeId = inodeId;
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
  
  public String getInodeName() {
    return inodeName;
  }
  
  public void setInodeName(String inodeName) {
    this.inodeName = inodeName;
  }
  
  public static List<ProvFileOpsSummaryByApp> summary(List<FileOp> fileOps) {
    Map<Long, ProvFileOpsSummaryByApp> byFileMap = new HashMap<>();
    Map<Pair<Long, String>, CompactApp> byFileAndAppMap = new HashMap<>();
    for(FileOp fileOp : fileOps) {
      ProvFileOpsSummaryByApp byFile = byFileMap.get(fileOp.getInodeId());
      if(byFile == null) {
        byFile = new ProvFileOpsSummaryByApp(fileOp.getInodeId(), fileOp.getInodeName());
        byFileMap.put(fileOp.getInodeId(), byFile);
      }
      Pair<Long, String> id = Pair.with(fileOp.getInodeId(), fileOp.getAppId());
      CompactApp byFileAndApp = byFileAndAppMap.get(id);
      if(byFileAndApp == null) {
        byFileAndApp = new CompactApp(fileOp.getAppId());
        byFileAndAppMap.put(id, byFileAndApp);
        byFile.addApp(byFileAndApp);
      }
      byFileAndApp.addOp(fileOp);
    }
    List<ProvFileOpsSummaryByApp> result = new LinkedList<>();
    for (ProvFileOpsSummaryByApp compactOps : byFileMap.values()) {
      compactOps.sortOpsByTimestamp();
      result.add(compactOps);
    }
    return result;
  }
  
  public void sortOpsByTimestamp() {
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
  
  @XmlRootElement
  public static class CompactApp {
    private String appId;
    private Long firstOpTimestamp;
    private Integer firstOpLogicalTime;
    private Set<String> ops = new HashSet<>();
    
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
  
    public Set<String> getOps() {
      return ops;
    }
  
    public void setOps(Set<String> ops) {
      this.ops = ops;
    }
  
    public void addOp(FileOp op) {
      ops.add(op.getInodeOperation());
      if(firstOpTimestamp < op.getTimestamp()) {
        firstOpTimestamp = op.getTimestamp();
      }
      if(firstOpLogicalTime < op.getLogicalTime()) {
        firstOpLogicalTime = op.getLogicalTime();
      }
    }
  }
}
