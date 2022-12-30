package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.remote.ShutDownAssociation;
import de.ddm.IndexClassColumn;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}


	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<DependencyMiner.Message> dependencyMinerLargeMessageProxy;
		int task;
		IndexClassColumn referencedVal;
		IndexClassColumn dependencyVal;
		int colThis;
		int colThat;


	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class tempMessage implements Message {
		private static final long serialVersionUID = 5128375631926163648L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		IndexClassColumn referencedVal;
		IndexClassColumn dependencyVal;
		String[] valuesRef;
		String[] valuesDep;
		int result;
	}
	//shutdown message is missing! --> added know!
	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = -1208833352862186050L;
	}




	////////////////////////
	// Actor Construction //
	////////////////////////

	//private final Map<IndexClassColumn, IndexClassColumn> referencedCols = new HashMap<>();
	private final Map<IndexClassColumn, String[]> referencedValues = new HashMap<>();
	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(tempMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle) // shutdown !
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		this.getContext().getLog().info("Work in progress!");
		boolean isRefCol = this.referencedValues.containsKey(message.getReferencedVal());
		IndexClassColumn refCol= message.getReferencedVal();
		IndexClassColumn depCol = message.getDependencyVal();
		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.RequestDataMessage(this.largeMessageProxy, refCol, depCol, message.colThis, message.colThat, isRefCol, message.getTask());
		message.dependencyMinerLargeMessageProxy.tell((LargeMessageProxy.Message) completionMessage);
		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		return Behaviors.stopped();
	}

	private Behavior<Message> handle(tempMessage message) {
		IndexClassColumn rv = message.getReferencedVal();
		int result = message.getResult();
		this.referencedValues.clear();
		this.referencedValues.put(rv, message.getValuesRef());
		this.getContext().getLog().info((this.referencedValues.toString()));
		String[] refCol = this.referencedValues.get(message.getReferencedVal());
		String[] depCol = message.valuesDep;
		boolean bTemp = false;
		for (String someStr : depCol) {
			if (0 > Arrays.binarySearch(refCol, someStr)) {bTemp = false; break;} else {bTemp = true;}
		}
		DependencyMiner.CompletionMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), message.getResult(), message.getReferencedVal(), message.getDependencyVal(), bTemp);
		message.getDependencyMinerLargeMessageProxy().tell((LargeMessageProxy.Message) completionMessage);
		return this;
	}
}
