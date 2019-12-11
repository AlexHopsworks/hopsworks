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
package io.hops.hopsworks.common.provenance.elastic.prov;

import io.hops.hopsworks.common.provenance.core.Provenance;
import io.hops.hopsworks.common.provenance.core.apiToElastic.ProvParser;
import io.hops.hopsworks.common.provenance.util.functional.CheckedFunction;
import io.hops.hopsworks.exceptions.ProvenanceException;
import io.hops.hopsworks.restutils.RESTCodes;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

public class ProvHelper {
  
  public static CheckedFunction<Object, Long, ProvenanceException> asLong(boolean soft) {
    return (Object val) -> {
      if(val == null) {
        if(soft) {
          return null;
        } else {
          throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.MALFORMED_ENTRY, Level.INFO,
            "expected Long, found null");
        }
      }
      return ((Number) val).longValue();
    };
  }
  
  public static CheckedFunction<Object, Integer, ProvenanceException> asInt(boolean soft) {
    return (Object val) -> {
      if(val == null) {
        if(soft) {
          return null;
        } else {
          throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.MALFORMED_ENTRY, Level.INFO,
            "expected Integer, found null");
        }
      }
      return ((Number) val).intValue();
    };
  }
  
  public static CheckedFunction<Object, String, ProvenanceException> asString(boolean soft) {
    return (Object val) -> {
      if(val == null) {
        if(soft) {
          return null;
        } else {
          throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.MALFORMED_ENTRY, Level.INFO,
            "expected String, found null");
        }
      }
      return val.toString();
    };
  }
  
  public static CheckedFunction<Object, Provenance.FileOps, ProvenanceException> asFileOp(boolean soft) {
    return (Object val) -> {
      if(val == null) {
        if(soft) {
          return null;
        } else {
          throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.MALFORMED_ENTRY, Level.INFO,
            "expected file op, found null");
        }
      }
      return Provenance.FileOps.valueOf((String)val);
    };
  }
  
  public static CheckedFunction<Object, ProvParser.DocSubType, ProvenanceException> asDocSubType(boolean soft) {
    return (Object val) -> {
      if(val == null) {
        if(soft) {
          return null;
        } else {
          throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.MALFORMED_ENTRY, Level.INFO,
            "expected doc sub type, found null");
        }
      }
      return ProvParser.DocSubType.valueOf((String)val);
    };
  }
  
  public static CheckedFunction<Object, Map<String, String>, ProvenanceException> asXAttrMap(boolean soft) {
    return (Object o) -> {
      Map<String, String> result = new HashMap<>();
      if(o == null) {
        if(soft) {
          return result;
        } else {
          throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.MALFORMED_ENTRY, Level.INFO,
            "expected xattr map, found null");
        }
      }
      Map<Object, Object> xattrsMap;
      try {
        xattrsMap = (Map) o;
      } catch (ClassCastException e) {
        throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.MALFORMED_ENTRY, Level.INFO,
          "prov xattr expected map object (1)", e.getMessage(), e);
      }
      for (Map.Entry<Object, Object> entry : xattrsMap.entrySet()) {
        String xattrKey;
        try {
          xattrKey = (String) entry.getKey();
        } catch (ClassCastException e) {
          throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.MALFORMED_ENTRY, Level.INFO,
            "prov xattr expected map with string keys", e.getMessage(), e);
        }
        String xattrVal;
        if (entry.getValue() instanceof Map) {
          Map<String, Object> xaMap = (Map) entry.getValue();
          if (xaMap.containsKey("raw")) {
            xattrVal = (String) xaMap.get("raw");
          } else {
            throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.MALFORMED_ENTRY, Level.INFO,
              "parsing prov xattr:" + entry.getKey());
          }
        } else if (entry.getValue() instanceof String) {
          xattrVal = (String) entry.getValue();
        } else {
          throw new ProvenanceException(RESTCodes.ProvenanceErrorCode.MALFORMED_ENTRY, Level.INFO,
            "prov xattr expected map or string");
        }
        result.put(xattrKey, xattrVal);
      }
      return result;
    };
  }
}
