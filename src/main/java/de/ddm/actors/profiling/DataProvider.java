package de.ddm.actors.profiling;
import akka.actor.typed.ActorRef;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;



public class DataProvider {

    //TODO: (col ref und col dep?) sinnvoll weiterleiten; depMiner verteilt an DepWorker; hier: inclusionDep verwenden (-> hashing!!!!)
    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -6164879298754451870L;
    }
    private ActorRef<DependencyMiner.Message> messageDepMiner;

    public DataProvider(ActorRef<DependencyMiner.Message> arg){
        this.messageDepMiner = arg;
    }
    public InclusionDependency isDep(DependencyMiner.Message messageDepMiner,DepMapper mapper) {

        return null; //TODO: implement
    }

}
