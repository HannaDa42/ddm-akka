package de.ddm.singletons.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.singletons.actors.patterns.Reaper;
import de.ddm.singletons.actors.profiling.DependencyWorker;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

public class Worker extends AbstractBehavior<Worker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "worker";

	public static Behavior<Message> create() {
		return Behaviors.setup(Worker::new);
	}

	private Worker(ActorContext<Message> context) {
		super(context);
		Reaper.watchWithDefaultReaper(this.getContext().getSelf());

		final int numWorkers = SystemConfigurationSingleton.get().getNumWorkers();

		this.workers = new ArrayList<>(numWorkers);
		for (int id = 0; id < numWorkers; id++)
			this.workers.add(context.spawn(DependencyWorker.create(), DependencyWorker.DEFAULT_NAME + "_" + id, DispatcherSelector.fromConfig("akka.worker-pool-dispatcher")));
	}

	/////////////////
	// Actor State //
	/////////////////

	final List<ActorRef<DependencyWorker.Message>> workers;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		//active child actors shutdown
		for(ActorRef<DependencyWorker.Message> dependencyWorker : this.workers) {dependencyWorker.tell(new DependencyWorker.ShutdownMessage());}
		return Behaviors.stopped();
	}
}
