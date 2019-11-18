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

import io.hops.hopsworks.common.util.HopsworksJAXBContext;
import io.hops.hopsworks.common.provenance.v3.xml.ProvCoreDTO;
import io.hops.hopsworks.common.provenance.v3.xml.ProvTypeDTO;
import io.hops.hopsworks.exceptions.GenericException;
import org.junit.Assert;
import org.junit.Test;

public class ConverterTest {
  @Test
  public void testTreeHelper() throws GenericException {
    HopsworksJAXBContext pc = new HopsworksJAXBContext();
    pc.init();
    ProvTypeDTO type = ProvTypeDTO.ProvType.MIN.dto;
    ProvCoreDTO core = new ProvCoreDTO(type, null);
    String json = pc.marshal(core);
    System.out.println(json);
    ProvCoreDTO coreAux = pc.unmarshal(json, ProvCoreDTO.class);
    Assert.assertEquals(core.getType(), coreAux.getType());
  }
}
