package de.ddm.actors.profiling;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.IndexUnaryIND;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.IndexClassColumn;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.extern.slf4j.Slf4j;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.io.File;
import java.util.*;

@Slf4j
public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	// state
	Map<Integer, List<Set<String>>> idContentMap = new HashMap<>();
	Map<Integer, Boolean> filteredMap = new HashMap<>();
	// actor ref
	Map<ActorRef<DependencyWorker.Message>, IndexClassColumn> actorColumnMap = new HashMap<>();
	Map<ActorRef<DependencyWorker.Message>, List<DependencyWorker.TaskMessage>> actorOccupationMap = new HashMap<>();
	// file representation


	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {									// C
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {																// C
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {																// C
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {																// C
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {														// C
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RequestDataMessage implements Message {															// C
		private static final long serialVersionUID = 868083729453247423L;
		ActorRef<LargeMessageProxy.Message> dependencyWorkerReceiverProxy;
		IndexClassColumn referencedColumnIdSingle;
		IndexClassColumn dependentColumnIdSingle;
		int dependentFrom;
		int dependentTo;
		boolean referencedColumn;
		int id;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {															// C
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		int id;
		IndexClassColumn referencedColumnIdSingle;
		IndexClassColumn dependentColumnIdSingle;
		boolean candidate;
	}

	//data request
	//shutdown message is missing!

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";														// C

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service"); // C

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}																													// C

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];
		this.fileRepresentation = new String[this.inputFiles.length][][];												// C
		///!!!!
		this.dataprov = null;

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));

	}

	/////////////////
	// Actor State //
	/////////////////
	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final String[][][] fileRepresentation;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;


	private final DataProvider dataprov;



	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onMessage(RequestDataMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		// Ignoring batch content for now ... but I could do so much with it.
		//TODO: implement meaningful messages here
		if (message.getBatch().size() != 0)
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			//TODO: Taskmessages weiterleiten; Hilfsfunktion implementieren!
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		InclusionDependency ind = this.dataprov.handle(message, (IndexClassColumn referencedColumnId, IndexClassColumn dependentColumnId) -> {
					File referencedFile = this.inputFiles[referencedColumnId.getFile()];
					File dependentFile = this.inputFiles[dependentColumnId.getFile()];
					String referencedAttribute = this.headerLines[referencedColumnId.getFile()][referencedColumnId.getColumn()];
					String dependentAttribute = this.headerLines[dependentColumnId.getFile()][dependentColumnId.getColumn()];
					return new InclusionDependency(dependentFile, new String[]{dependentAttribute}, referencedFile, new String[]{referencedAttribute});
				});


		this.resultCollector.tell(new ResultCollector.ResultMessage(Collections.singletonList(ind)));
		//TODO: Nary Deps beruecksichtigen!


		this.getContext().getLog().info("CompletionMessage:");
		return this;
	}

	private Behavior<Message> handle(RequestDataMessage message) {
		return this; //TODO: implement
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}


}