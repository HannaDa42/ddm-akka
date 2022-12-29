package de.ddm.actors.profiling;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.NoArgsConstructor;

public class DataProvider {

    // state
    //TODO: DataProvider muss daten(col ref und col dep?) an DepMiner durch inputReader sinnvoll weiterleiten; depMiner verteilt an DepWorker; DepWorker prüft InclusionDep

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -6164879298754451870L;
    }


    //data request
    //shutdown message is missing!

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "dataProvider";


    ////////////////////
    // Actor Behavior //
    ////////////////////

}
