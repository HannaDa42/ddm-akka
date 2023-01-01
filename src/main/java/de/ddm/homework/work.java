package de.ddm.homework;

import akka.actor.typed.ActorRef;
import de.ddm.singletons.actors.profiling.DependencyMiner;
import de.ddm.singletons.actors.profiling.DependencyWorker;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.*;
import java.util.function.Consumer;

// generate actors
@Getter
@AllArgsConstructor
public class work implements Iterator<DependencyWorker.TaskMessage> {
    //Jobs assignment list
    Map<InclusionHash, Integer> tempMap = new HashMap<>();
    //actor crashed before job done
    Queue<DependencyWorker.TaskMessage> crashedActorsList = new LinkedList<>();
    //Jobs to solve
    Queue<FileHash> actorQueue = new LinkedList<>();
    DependencyWorker.TaskMessage workload;
    final String[][][] fileColRefList;

    // Raspberry Pi Batch Size?
    int batchIndex = 0;
    int msgIndex = 0;
    final int batchSize = 5000;
    public FileHash referencedVal;

    //Dependency Miner
    final ActorRef<DependencyMiner.Message> depMinRef;


    public work(FileHash referencedVal, ActorRef<DependencyMiner.Message> depMinRef, String[][][] fileColRefList) {
        this.batchIndex = 0;
        this.referencedVal = referencedVal;
        this.depMinRef = depMinRef;
        this.fileColRefList = fileColRefList;
        //do work on this
        this.workload = worker_do_work();
    }

    public boolean assignBatch(InclusionHash indexActor, int jobNumber) {
        //assign Batch
        if (this.batchIndex == 0) { tempMap.put(indexActor, jobNumber); }
        if (tempMap.containsKey(indexActor)) { return true;
        } else { return false;}
    }

    private DependencyWorker.TaskMessage worker_do_work() {
        if (crashedActorsList.isEmpty() == false) {
            //load work that failed on another actor
            return crashedActorsList.poll();
        } else if (this.actorQueue.isEmpty()) {
            //no work anymore
            return null;
        } else {
            //Take head job of queue
            FileHash dependencyVal = this.actorQueue.peek();
            //Index for Actor
            InclusionHash indexActor = new InclusionHash(this.referencedVal, dependencyVal);
            //boolean check [relevant? or not? batch]
            boolean usefulBatch = false;


            //init dependent reference
            int fileIndex = dependencyVal.getFile();
            int fileColIndex = dependencyVal.getEntry();
            String[] depRef = fileColRefList[fileIndex][fileColIndex];
            int colSize = depRef.length;
            int jobNumber = (int) Math.ceil((double) colSize / batchSize);

            //assign batch for task
            usefulBatch = assignBatch(indexActor, jobNumber);
            int batchStart = batchIndex;
            int batchEnd = Math.min(batchIndex + batchSize, colSize);
            //build taskMessage
            DependencyWorker.TaskMessage msg = new DependencyWorker.TaskMessage(depMinRef, msgIndex++, referencedVal, dependencyVal, batchIndex, batchEnd);
            batchIndex = batchEnd;

            //check if batch is useful
            if (usefulBatch == false || batchIndex >= colSize) {
                this.batchIndex = 0;
                this.actorQueue.poll();
            }



            //recursive call until useful work found
            if (usefulBatch == false) { msg = this.worker_do_work(); }
            return msg;
        }
    }

    // standard iterator method
    @Override
    public boolean hasNext() {
        return false;
    }

    // standard iterator method
    @Override
    public DependencyWorker.TaskMessage next() {
        return null;
    }

    // standard iterator method
    @Override
    public void remove() {
        Iterator.super.remove();
    }

    // standard iterator method
    @Override
    public void forEachRemaining(Consumer<? super DependencyWorker.TaskMessage> action) {
        Iterator.super.forEachRemaining(action);
    }
}