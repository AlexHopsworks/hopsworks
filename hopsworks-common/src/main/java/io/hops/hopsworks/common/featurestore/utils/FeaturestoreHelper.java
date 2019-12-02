package io.hops.hopsworks.common.featurestore.utils;

import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.util.Settings;

public class FeaturestoreHelper {
  public static String getHiveDBName(Project project) {
    return project.getName() + ".db";
  }
  
  public static String getHiveDBPath(Settings settings, Project project) {
    return settings.getHiveWarehouse() + "/" + getHiveDBName(project);
  }
  
  public static String getFeaturestoreName(Project project) {
    return project.getName() + "_featurestore.db";
  }
  
  public static String getFeaturestorePath(Settings settings, Project project) {
    return settings.getHiveWarehouse() + "/" + getFeaturestoreName(project);
  }
  
  public static String getFeaturegroupName(String featuregroupName, Integer version) {
    return featuregroupName + "_" + version.toString();
  }
}
