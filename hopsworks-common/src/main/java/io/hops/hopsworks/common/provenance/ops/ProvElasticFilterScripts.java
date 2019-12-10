package io.hops.hopsworks.common.provenance.ops;

import org.elasticsearch.script.Script;

public class ProvElasticFilterScripts {
  public enum Scripts {
    ;
    public final Script script;
    Scripts(Script script) {
      this.script = script;
    }
  }
}
