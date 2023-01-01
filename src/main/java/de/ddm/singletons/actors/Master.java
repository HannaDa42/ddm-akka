package de.ddm.singletons.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.singletons.actors.patterns.Reaper;
import de.ddm.singletons.actors.profiling.DependencyMiner;
import de.ddm.serialization.AkkaSerializable;
import lombok.NoArgsConstructor;

public class Master extends AbstractBehavior<Master.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "master";

	public static Behavior<Message> create() {
		return Behaviors.setup(Master::new);
	}

	private Master(ActorContext<Message> context) {
		super(context);
		Reaper.watchWithDefaultReaper(this.getContext().getSelf());

		this.dependencyMiner = context.spawn(DependencyMiner.create(), DependencyMiner.DEFAULT_NAME, DispatcherSelector.fromConfig("akka.master-pinned-dispatcher"));
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<DependencyMiner.Message> dependencyMiner;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		this.dependencyMiner.tell(new DependencyMiner.StartMessage());
		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		//dependencyMiner shutdown!!!!!
		this.dependencyMiner.tell(new DependencyMiner.ShutdownMessage());
		return Behaviors.stopped();
	}
}