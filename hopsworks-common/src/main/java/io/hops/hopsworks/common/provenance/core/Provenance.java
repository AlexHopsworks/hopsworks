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
package io.hops.hopsworks.common.provenance.core;

import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.util.Settings;

public class Provenance {
  public enum FileOps {
    CREATE,
    DELETE,
    ACCESS_DATA,
    MODIFY_DATA;
  }
  public enum MLType {
    FEATURE,
    TRAINING_DATASET,
    EXPERIMENT,
    MODEL,
    HIVE,
    DATASET,
    NONE
  }
  public enum AppState {
    SUBMITTED,
    RUNNING,
    FINISHED,
    KILLED,
    FAILED,
    UNKNOWN;
    
    public boolean isFinalState() {
      switch(this) {
        case FINISHED:
        case KILLED:
        case FAILED:
          return true;
        default: 
          return false;
      } 
    }
  }
  
  public enum FootprintType {
    ALL,
    INPUT,            //files read by the application
    OUTPUT_MODIFIED,  //existing files modified by application (not created or deleted)
    OUTPUT_ADDED,     //files newly created by application
    OUTPUT_TMP,              //files created and deleted by application
    OUTPUT_REMOVED
  }
  
  public static String getProjectIndex(Project project) {
    return project.getInode().getId() + Settings.PROV_FILE_INDEX_SUFFIX;
  }
}
