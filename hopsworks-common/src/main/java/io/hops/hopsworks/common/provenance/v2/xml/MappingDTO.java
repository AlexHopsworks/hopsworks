package io.hops.hopsworks.common.provenance.v2.xml;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Map;

@XmlRootElement
public class MappingDTO {
  private String index;
  private Map<String, String> mapping;
  
  public MappingDTO() {}
  
  public MappingDTO(String index, Map<String, String> mapping) {
    this.index = index;
    this.mapping = mapping;
  }
  
  public String getIndex() {
    return index;
  }
  
  public void setIndex(String index) {
    this.index = index;
  }
  
  public Map<String, String> getMapping() {
    return mapping;
  }
  
  public void setMapping(Map<String, String> mapping) {
    this.mapping = mapping;
  }
}
