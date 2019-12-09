package io.hops.hopsworks.common.provenance.xml;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Map;

@XmlRootElement
public class ElasticIndexMappingDTO {
  private String index;
  private Map<String, String> mapping;
  
  public ElasticIndexMappingDTO() {}
  
  public ElasticIndexMappingDTO(String index, Map<String, String> mapping) {
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
