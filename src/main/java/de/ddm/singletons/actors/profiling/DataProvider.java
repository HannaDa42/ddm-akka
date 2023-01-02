package de.ddm.singletons.actors.profiling;
import akka.actor.typed.ActorRef;
import de.ddm.homework.FileHash;
import de.ddm.homework.InclusionHash;
import de.ddm.singletons.actors.patterns.LargeMessageProxy;
import de.ddm.homework.work;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.InclusionDependency;
import lombok.NoArgsConstructor;

import java.util.*;


public class DataProvider {

    public FileHash nextRefHash() {
        FileHash next = this.nextRef.poll();
        this.nextRef.offer(next);
        return next;
    }

    public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
    }
    private ActorRef<DependencyMiner.Message> messageDepMiner;
    Map<InclusionHash, Integer> tempMap = new HashMap<>();
    private final String[][][] fileRef;
    Map<FileHash, work> IndexMap = new HashMap<>();

    @NoArgsConstructor
    public static class StartMessage implements Message {
        private static final long serialVersionUID = -6164879298754451870L;
    }


    public DataProvider(ActorRef<DependencyMiner.Message> arg, String[][][] file){
        this.messageDepMiner = arg;
        this.fileRef = file;
    }


    Queue<FileHash> nextRef = new LinkedList<>();
    public boolean new_job_bool() {
        boolean new_task = this.nextRef.stream().anyMatch((id) -> this.IndexMap.get(id).hasNext());
        return new_task;
    }

    public DependencyWorker.TaskMessage new_job(FileHash index) {
        DependencyWorker.TaskMessage msg = this.IndexMap.get(index).next();
        if (!this.IndexMap.get(index).hasNext()) {nextRef.remove(index);}
        return msg;
    }

    public InclusionDependency handle(DependencyMiner.CompletionMessage messageDepMiner,DepMapper mapper) {
        InclusionHash indexedHash = new InclusionHash(messageDepMiner.getRefHash(),messageDepMiner.getDepHash());
        if(messageDepMiner.isCandidate()) {
            if (tempMap.remove(indexedHash) != null) {
                if ((tempMap.remove(indexedHash)-1)==0) {
                    return mapper.isDep(indexedHash.getRefTable(), indexedHash.getDepTable());
                }
                tempMap.put(indexedHash,(tempMap.remove(indexedHash)-1));
                return null;
            }
        }
        tempMap.remove(indexedHash);
        return null;
    }

    Queue<FileHash> nextColQ = new PriorityQueue<>();
    Queue<FileHash> unmatchedColQ = new LinkedList<>();
    public void add_data (int id, int noCols) {
        for (int i = 0; i < noCols; i++) {
            FileHash colId = new FileHash(id, i);
            for (work un : this.IndexMap.values()) {
                if (colId.getFile() != un.referencedVal.getFile()) {
                    this.unmatchedColQ.offer(colId);
                }
            }
            work uindTemp = new work(colId,messageDepMiner,fileRef);
            for (FileHash dep : this.IndexMap.keySet()) {
                if (dep.getFile() != uindTemp.referencedVal.getFile()) {
                    this.unmatchedColQ.offer(dep);
                }
            }
            if (!(this.IndexMap.containsKey(colId))){
                this.IndexMap.put(colId, uindTemp);
            }
            this.nextColQ.clear();
            this.nextColQ.addAll(this.IndexMap.keySet());
        }
    }
}
