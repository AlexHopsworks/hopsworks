package io.hops.hopsworks.kmon.settings;

import java.io.Serializable;
import java.util.logging.Logger;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;
import io.hops.hopsworks.common.dao.host.Status;

@ManagedBean
@SessionScoped
public class SettingsController implements Serializable {

  private static final Logger logger = Logger.getLogger(
          SettingsController.class.getName());
  private String name;
  private int logLines;

  public SettingsController() {
    logger.info("SettingsController");
    name = "Hopsworks";
    logLines = 2;
  }

  public String getName() {
    return name;
  }

  public String tooltip(String id, int n) {

    if (id.equals(Status.TimedOut.toString())) {
      return n > 1 ? n
              + " services have been timed out (No heartbeat from the host)"
              : "1 service has been timed out (No heartbeat from the host)";
    }
    if (id.equals(Status.Stopped.toString())) {
      return n > 1 ? n + " services are not running" : "1 service is not running";
    }
    if (id.equals(Status.Started.toString())) {
      return n > 1 ? n + " services are running" : "1 service is running";
    }
    return "";
  }

  public String tooltip(String id) {

    if (id.equals(Status.TimedOut.toString())) {
      return "The service instance has been timed out: no heartbeat from the host";
    }
    if (id.equals(Status.Stopped.toString())) {
      return "service is not running";
    }
    if (id.equals(Status.Started.toString())) {
      return "service is running";
    }
    return "";
  }

  public String tooltipDisabledAction(String action) {
    if (action.equalsIgnoreCase("start")) {
      return "You may only start a stopped service";
    }
    if (action.equalsIgnoreCase("stop")) {
      return "You may only stop a started service";
    }
    return "";
  }

}
