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

import io.hops.hopsworks.common.dao.hdfs.inode.Inode;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.exceptions.GenericException;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

public class TreeHelper {
  public static <S extends ProvenanceController.BasicFileState> void merge(
    ProvenanceController.BasicTreeBuilder<S> to, ProvenanceController.BasicTreeBuilder<S> from) {
    if(to.getInodeId() == null) {
      to.setInodeId(from.getInodeId());
    }
    if(to.getName() == null) {
      to.setName(from.getName());
    }
    if(to.getFileState() == null) {
      to.setFileState(from.getFileState());
    }
  }
  
  public static class TreeStruct<S extends ProvenanceController.BasicFileState> {
    Supplier<ProvenanceController.BasicTreeBuilder<S>> instanceBuilder;
    
    ArrayList<Long> findInInodes = new ArrayList<>();
    ArrayList<Long> pendingInInodes = new ArrayList<>();
    ArrayList<Long> findInProvenance = new ArrayList<>();
    ArrayList<Long> pendingInProvenance = new ArrayList<>();
    ArrayList<Long> notFound = new ArrayList<>();
    TreeMap<Long, ProvenanceController.BasicTreeBuilder<S>> allNodes = new TreeMap<>();
    TreeMap<Long, ProvenanceController.BasicTreeBuilder<S>> incompleteNodes = new TreeMap<>();
    TreeMap<Long, ProvenanceController.BasicTreeBuilder<S>> projectNodes = new TreeMap<>();
  
    public TreeStruct(Supplier<ProvenanceController.BasicTreeBuilder<S>> instanceBuilder) {
      this.instanceBuilder = instanceBuilder;
    }
    
    public Pair<Map<Long, ProvenanceController.BasicTreeBuilder<S>>,
      Map<Long, ProvenanceController.BasicTreeBuilder<S>>> getMinTree() {
      return Pair.with(incompleteNodes, new HashMap<>());
    }
  
    public Pair<Map<Long, ProvenanceController.BasicTreeBuilder<S>>,
      Map<Long, ProvenanceController.BasicTreeBuilder<S>>> getFullTree() {
      return Pair.with(projectNodes, incompleteNodes);
    }
    
    public void processBasicFileState(List<S> fileStates) throws GenericException {
      for (S fileState : fileStates) {
        if (fileState.isProject()) {
          ProvenanceController.BasicTreeBuilder<S> projectNode = getOrBuildProjectNode(fileState);
          projectNode.setFileState(fileState);
        } else {
          ProvenanceController.BasicTreeBuilder<S> parentNode = getOrBuildParentNode(fileState);
          ProvenanceController.BasicTreeBuilder<S> node = getOrBuildNode(fileState);
          parentNode.addChild(node);
        }
      }
    }
    
    private ProvenanceController.BasicTreeBuilder<S> getOrBuildProjectNode(S fileState) {
      ProvenanceController.BasicTreeBuilder<S> projectNode = projectNodes.get(fileState.getProjectInodeId());
      if (projectNode == null) {
        projectNode = instanceBuilder.get();
        projectNode.setInodeId(fileState.getProjectInodeId());
        allNodes.put(projectNode.getInodeId(), projectNode);
        projectNodes.put(projectNode.getInodeId(), projectNode);
        findInInodes.add(projectNode.getInodeId());
      }
      return projectNode;
    }
  
    private ProvenanceController.BasicTreeBuilder<S> getOrBuildParentNode(S fileState) {
      getOrBuildProjectNode(fileState);
      ProvenanceController.BasicTreeBuilder<S> parentNode = getOrBuildParentNode(fileState.getParentInodeId());
      return parentNode;
    }
    
    private ProvenanceController.BasicTreeBuilder<S> getOrBuildParentNode(Long parentInodeId) {
      ProvenanceController.BasicTreeBuilder<S> parentNode = allNodes.get(parentInodeId);
      if (parentNode == null) {
        parentNode = instanceBuilder.get();
        parentNode.setInodeId(parentInodeId);
        allNodes.put(parentNode.getInodeId(), parentNode);
        if(!projectNodes.containsKey(parentNode.getInodeId())) {
          findInInodes.add(parentNode.getInodeId());
          incompleteNodes.put(parentNode.getInodeId(), parentNode);
        }
      }
      return parentNode;
    }
  
    private ProvenanceController.BasicTreeBuilder<S> getOrBuildNode(S fileState) {
      incompleteNodes.remove(fileState.getInodeId());
      findInInodes.remove(fileState.getInodeId());
      ProvenanceController.BasicTreeBuilder<S> node = allNodes.get(fileState.getInodeId());
      if (node == null) {
        node = instanceBuilder.get();
        node.setInodeId(fileState.getInodeId());
        allNodes.put(node.getInodeId(), node);
      }
      node.setName(fileState.getInodeName());
      node.setFileState(fileState);
      return node;
    }
  
    public boolean findInInodes() {
      return !findInInodes.isEmpty();
    }
  
    public boolean findInProvenance() {
      return !findInProvenance.isEmpty();
    }
    
    public boolean complete() {
      return findInInodes.isEmpty() && findInProvenance.isEmpty();
    }
    
    public List<Long> nextFindInInodes() {
      int batchSize = Math.min(100, findInInodes.size());
      List<Long> batch = new ArrayList<>(findInInodes.subList(0, batchSize));
      findInInodes.removeAll(batch);
      pendingInInodes.addAll(batch);
      return batch;
    }
    
    public void processInodeBatch(List<Long> inodeBatch, List<Inode> inodes) throws GenericException {
      pendingInInodes.removeAll(inodeBatch);
      Set<Long> inodesNotFound = new HashSet<>(inodeBatch);
      for(Inode inode : inodes) {
        inodesNotFound.remove(inode.getId());
        ProvenanceController.BasicTreeBuilder<S> node = incompleteNodes.remove(inode.getId());
        if(node != null) {
          node.setName(inode.getInodePK().getName());
          ProvenanceController.BasicTreeBuilder<S> parentNode = getOrBuildParentNode(inode.getInodePK().getParentId());
          parentNode.addChild(node);
        } else {
          node = projectNodes.get(inode.getId());
          if(node != null) {
            node.setName(inode.getInodePK().getName());
          }
        }
      }
      findInProvenance.addAll(inodesNotFound);
    }
    
    public void processProvenanceBatch(List<Long> inodeBatch, List<FileOp> inodes) throws GenericException {
      pendingInProvenance.removeAll(inodeBatch);
      Set<Long> inodesNotFound = new HashSet<>(inodeBatch);
      for(FileOp inode : inodes) {
        inodesNotFound.remove(inode.getInodeId());
        ProvenanceController.BasicTreeBuilder<S> node = incompleteNodes.remove(inode.getId());
        if(node != null) {
          node.setName(inode.getInodeName());
          ProvenanceController.BasicTreeBuilder<S> parentNode = getOrBuildParentNode(inode.getParentInodeId());
          parentNode.addChild(node);
        } else {
          node = projectNodes.get(inode.getId());
          if(node != null) {
            node.setName(inode.getInodeName());
          }
        }
      }
      notFound.addAll(inodesNotFound);
    }
    
    public List<Long> nextFindInProvenance() {
      int batchSize = Math.min(100, findInProvenance.size());
      List<Long> batch = new ArrayList<>(findInProvenance.subList(0, batchSize));
      findInProvenance.removeAll(batch);
      pendingInProvenance.addAll(batch);
      return batch;
    }
  }
}
