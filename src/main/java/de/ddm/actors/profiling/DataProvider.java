package de.ddm.actors.profiling;
import akka.actor.typed.ActorRef;
import de.ddm.IndexClassColumn;
import de.ddm.IndexUnaryIND;
import de.ddm.UnaryIND;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.InclusionDependency;
import lombok.NoArgsConstructor;

import java.util.*;


public class DataProvider {

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }
    private ActorRef<DependencyMiner.Message> messageDepMiner;
    Map<IndexUnaryIND, Integer> tempMap = new HashMap<>();
    private final String[][][] fileRef;
    Map<IndexClassColumn, UnaryIND> indDistributor = new HashMap<>();

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -6164879298754451870L;
    }


    public DataProvider(ActorRef<DependencyMiner.Message> arg, String[][][] file, UnaryIND indDistributor){
        this.messageDepMiner = arg;
        this.fileRef = file;
    }

    public InclusionDependency handle(DependencyMiner.CompletionMessage messageDepMiner,DepMapper mapper) {
        IndexUnaryIND indexedId = new IndexUnaryIND(messageDepMiner.getReferencedColumnIdSingle(),messageDepMiner.getDependentColumnIdSingle());
        if(messageDepMiner.isCandidate()) {
            if (tempMap.remove(indexedId) != null) {
                if ((tempMap.remove(indexedId)-1)==0) {
                    return mapper.isDep(indexedId.getReferencedIndex(), indexedId.getDependentIndex());
                }
                tempMap.put(indexedId,(tempMap.remove(indexedId)-1));
                return null;
            }
        }
        tempMap.remove(indexedId);
        return null;
    }

    Queue<IndexClassColumn> nextColQ = new PriorityQueue<>();
    Queue<IndexClassColumn> unmatchedColQ = new LinkedList<>();
    public int add_data (int id, int noCols) {
        for (int i = 0; i < noCols; i++) {
            IndexClassColumn colId = new IndexClassColumn(id, i);
            for (UnaryIND un : this.indDistributor.values()) {
                if (colId.getFile() != un.referencedVal.getFile()) {
                    this.unmatchedColQ.offer(colId);
                }
            }
            UnaryIND uindTemp = new UnaryIND(colId,messageDepMiner,fileRef);
            for (IndexClassColumn dep : this.indDistributor.keySet()) {
                if (dep.getFile() != uindTemp.referencedVal.getFile()) {
                    this.unmatchedColQ.offer(dep);
                }
            }
            this.indDistributor.put(colId, uindTemp);
            this.nextColQ.clear();
            this.nextColQ.addAll(this.indDistributor.keySet());
        }
        return 0;
    }
}
