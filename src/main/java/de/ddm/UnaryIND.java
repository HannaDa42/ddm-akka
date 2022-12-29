package de.ddm;
import akka.actor.typed.ActorRef;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.structures.InclusionDependency;
import java.util.*;

// generate task and distribute
public class UnaryIND {

    private final String[][][] contents;
    Map<IndexUnaryIND, Integer> taskTrackerMap = new HashMap<>();
    public interface InclusionDependencyMapper { InclusionDependency map(IndexClassColumn referencedColumnId, IndexClassColumn dependentColumnId);}



}