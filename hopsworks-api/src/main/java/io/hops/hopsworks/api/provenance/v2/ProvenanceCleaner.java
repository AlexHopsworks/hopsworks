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
package io.hops.hopsworks.api.provenance.v2;

import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.v2.xml.ArchiveDTO;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Timer;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.util.logging.Level;
import java.util.logging.Logger;

@Singleton
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProvenanceCleaner {
  private final static Logger LOGGER = Logger.getLogger(ProvenanceCleaner.class.getName());

  @EJB
  private ProvenanceController provenanceCtrl;
  @EJB
  private Settings settings;
  
  private String lastIndexChecked = "";
  
  // Run once per hour
  @Schedule(persistent = false, hour = "*", minute = "*")
  public void execute(Timer timer) {
    int cleanupSize = settings.getProvCleanupSize();
    int archiveSize = settings.getProvArchiveSize();
    if(archiveSize == 0) {
      return;
    }
    try {
      Pair<ArchiveDTO.Round, String> round = provenanceCtrl.archiveRound(lastIndexChecked, cleanupSize, archiveSize);
      LOGGER.log(Level.INFO, "cleanup round - operations archived:{0} idx cleaned:{1} from:{2} to:{3}",
        new Object[]{round.getValue0().getArchived(), round.getValue0().getCleaned(), lastIndexChecked,
          round.getValue1()});
      lastIndexChecked = round.getValue1();
    } catch (GenericException | ServiceException e) {
      LOGGER.log(Level.INFO, "cleanup round was not successful - error", e);
    }
  }
}
