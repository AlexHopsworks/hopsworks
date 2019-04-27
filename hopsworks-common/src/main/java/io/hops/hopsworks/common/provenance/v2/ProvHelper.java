/*
 * Changes to this file committed after and not including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
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
 *
 * Changes to this file committed before and including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
 * Copyright (C) 2013 - 2018, Logical Clocks AB and RISE SICS AB. All rights reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS  OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package io.hops.hopsworks.common.provenance.v2;

import io.hops.hopsworks.common.provenance.util.CheckedFunction;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

public class ProvHelper {
  public static CheckedFunction<Object, Long, GenericException> asLong(boolean soft) {
    return (Object val) -> {
      if(val == null) {
        if(soft) {
          return null;
        } else {
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
            "expected Long, found null");
        }
      }
      return ((Number) val).longValue();
    };
  }
  
  public static CheckedFunction<Object, Integer, GenericException> asInt(boolean soft) {
    return (Object val) -> {
      if(val == null) {
        if(soft) {
          return null;
        } else {
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
            "expected Integer, found null");
        }
      }
      return ((Number) val).intValue();
    };
  }
  
  public static CheckedFunction<Object, String, GenericException> asString(boolean soft) {
    return (Object val) -> {
      if(val == null) {
        if(soft) {
          return null;
        } else {
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
            "expected String, found null");
        }
      }
      return val.toString();
    };
  }
  
  public static CheckedFunction<Object, Map<String, String>, GenericException> asXAttrMap(boolean soft) {
    return (Object o) -> {
      Map<String, String> result = new HashMap<>();
      if(o == null) {
        if(soft) {
          return result;
        } else {
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
            "expected xattr map, found null");
        }
      }
      Map<Object, Object> xattrsMap;
      try {
        xattrsMap = (Map) o;
      } catch (ClassCastException e) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "prov xattr expected map object (1)", e.getMessage(), e);
      }
      for (Map.Entry<Object, Object> entry : xattrsMap.entrySet()) {
        String xattrKey;
        try {
          xattrKey = (String) entry.getKey();
        } catch (ClassCastException e) {
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
            "prov xattr expected map with string keys", e.getMessage(), e);
        }
        String xattrVal;
        if (entry.getValue() instanceof Map) {
          Map<String, Object> xaMap = (Map) entry.getValue();
          if (xaMap.containsKey("raw")) {
            xattrVal = (String) xaMap.get("raw");
          } else {
            throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
              "parsing prov xattr:" + entry.getKey());
          }
        } else if (entry.getValue() instanceof String) {
          xattrVal = (String) entry.getValue();
        } else {
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
            "prov xattr expected map or string");
        }
        result.put(xattrKey, xattrVal);
      }
      return result;
    };
  }
}
