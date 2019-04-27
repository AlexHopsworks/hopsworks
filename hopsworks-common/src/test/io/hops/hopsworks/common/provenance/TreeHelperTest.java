///*
// * This file is part of Hopsworks
// * Copyright (C) 2018, Logical Clocks AB. All rights reserved
// *
// * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
// * the GNU Affero General Public License as published by the Free Software Foundation,
// * either version 3 of the License, or (at your option) any later version.
// *
// * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
// * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
// * PURPOSE.  See the GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License along with this program.
// * If not, see <https://www.gnu.org/licenses/>.
// */
//package io.hops.hopsworks.common.provenance;
//
//import io.hops.hopsworks.common.provenance.v2.xml.FootprintFileState;
//import io.hops.hopsworks.common.provenance.v2.xml.FootprintFileStateTree;
//import io.hops.hopsworks.common.provenance.v2.xml.TreeHelper;
//import io.hops.hopsworks.exceptions.GenericException;
//import org.junit.Test;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import static junit.framework.Assert.assertEquals;
//
//public class TreeHelperTest {
//  @Test
//  public void testTreeHelper() throws GenericException {
//    TreeHelper.TreeStruct<FootprintFileState> collector
//      = new TreeHelper.TreeStruct<>(() -> new FootprintFileStateTree());
//    Map<Long, FootprintFileState> fileStates = new HashMap<>();
//    fileStates.put(1l, fState(1l, 0l, "file1"));
//    fileStates.put(2l, fState(2l, 0l, "file2"));
//    fileStates.put(4l, fState(4l, 3l, "file3"));
//    collector.processBasicFileState(fileStates);
//    assertEquals(2, collector.getMinTree().getValue0().size());
//  }
//
//  private FootprintFileState fState(Long fileInodeId, Long parentInodeId, String fileName) {
//    FootprintFileState state = new FootprintFileState();
//    state.setInodeId(fileInodeId);
//    state.setInodeName(fileName);
//    state.setParentInodeId(parentInodeId);
//    return state;
//  }
//}
