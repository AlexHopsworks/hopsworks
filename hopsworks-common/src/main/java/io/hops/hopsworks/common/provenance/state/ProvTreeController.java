package io.hops.hopsworks.common.provenance.state;

import io.hops.hopsworks.common.dao.hdfs.inode.Inode;
import io.hops.hopsworks.common.dao.hdfs.inode.InodeFacade;
import io.hops.hopsworks.exceptions.ProvenanceException;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProvTreeController {
  
  @EJB
  private InodeFacade inodeFacade;
  
  public <S extends ProvTree.State> Pair<Map<Long, ProvTree.Builder<S>>, Map<Long, ProvTree.Builder<S>>>
    processAsTree(List<S> fileStates, Supplier<ProvTree.Builder<S>> instanceBuilder, boolean fullTree)
    throws ProvenanceException {
    ProvTree.AuxStruct<S> treeS = new ProvTree.AuxStruct<S>(instanceBuilder);
    treeS.processBasicFileState(fileStates);
    if(fullTree) {
      int maxDepth = 100;
      while(!treeS.complete() && maxDepth > 0 ) {
        maxDepth--;
        while (treeS.findInInodes()) {
          List<Long> inodeIdBatch = treeS.nextFindInInodes();
          List<Inode> inodeBatch = inodeFacade.findByIdList(inodeIdBatch);
          treeS.processInodeBatch(inodeIdBatch, inodeBatch);
        }
      }
      return treeS.getFullTree();
    } else {
      return treeS.getMinTree();
    }
  }
}
