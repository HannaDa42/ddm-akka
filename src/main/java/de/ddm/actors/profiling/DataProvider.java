package de.ddm.actors.profiling;
import akka.actor.typed.ActorRef;
import de.ddm.IndexClassColumn;
import de.ddm.IndexUnaryIND;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;


public class DataProvider {

    //TODO: (col ref und col dep?) sinnvoll weiterleiten; depMiner verteilt an DepWorker; hier: inclusionDep verwenden (-> hashing!!!!)
    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -6164879298754451870L;
    }
    private ActorRef<DependencyMiner.Message> messageDepMiner;
    Map<IndexUnaryIND, Integer> tempMap = new HashMap<>();

    public DataProvider(ActorRef<DependencyMiner.Message> arg){
        this.messageDepMiner = arg;
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

}
