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

	// state of work
	Map<Integer, List<Set<String>>> idContentMap = new HashMap<>();
	Map<Integer, Boolean> filteredMap = new HashMap<>();
	// actor ref
	Map<ActorRef<DependencyWorker.Message>, IndexClassColumn> actorColumnMap = new HashMap<>();
	Map<ActorRef<DependencyWorker.Message>, List<DependencyWorker.TaskMessage>> actorOccupationMap = new HashMap<>();
	// file representation


	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RequestDataMessage implements Message {
		private static final long serialVersionUID = 868083729453247423L;
		ActorRef<LargeMessageProxy.Message> dependencyWorkerReceiverProxy;
		IndexClassColumn refIndex;
		IndexClassColumn depIndex;
		int depStart;
		int depEnd;
		boolean refBool;
		int id;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		int id;
		IndexClassColumn refIndex;
		IndexClassColumn depIndex;
		boolean candidate;
	}

	//TODO: shutdown message is missing! --> added know!
	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 294532486808377423L;
	}


	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {return Behaviors.setup(DependencyMiner::new);}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];
		this.fileRepresentation = new String[this.inputFiles.length][][];
		///TODO: !!!!
		this.dataprov = new DataProvider(this.getContext().getSelf(), fileRepresentation, indDistributor);
		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++){this.idContentMap.put(id, new ArrayList<>());
			this.filteredMap.put(id, false);
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));}
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




	////////////////////
	// Actor Behavior //
	////////////////////


	private final DataProvider dataprov;

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle) //TODO: shutdown ! -> done
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

	private boolean insertFullTree(int jobID, int listSize) {
		if(jobID < listSize) {return true;
		} else {return false;}
	}

	private Behavior<Message> handle(BatchMessage message) {
		//TODO: check batchMsgTree
		this.getContext().getLog().info("BatchMessage ID {}", message.id);
		//
		int batchSize = message.getBatch().size();
		//read file obj
		if(batchSize > 0) {
			final int jobLeng = message.batch.get(0).length;

			for(int i = 0; i < jobLeng; i++) {
				Set<String> batchMsgTree = new TreeSet<>();
				//build the batch set
				for(int j = 0; j < batchSize; j++) {batchMsgTree.add(message.batch.get(j)[i]);}
				List<Set<String>> list = idContentMap.get(message.id);


				//insert top element or full tree
				if(!insertFullTree(i, list.size())){list.add(batchMsgTree);
				} else {list.addAll(batchMsgTree);}
			}
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		//transform into array for batching
		} else {
			//boolean check
			filteredMap.put(message.id, true);
			List<Set<String>> contentMap = idContentMap.get(message.id);
			int mapSize = contentMap.size();
			//representing array
			fileRepresentation[message.id] = new String[mapSize][];

			for(int i = 0; i<mapSize; i++) {
				//transformation
				String[] data = contentMap.get(i).toArray(new String[0]);
				//sort for unary IND
				Arrays.sort(data);
				fileRepresentation[message.id][i] = data;
			}

			this.getContext().getLog().info("BatchMessage ID {} loaded data!", message.id);
			//send data
			int msgSize = this.idContentMap.get(message.id).size();
			this.dataprov.add_data(message.id, msgSize);
			this.idContentMap.remove(message.id);
		}
		//stay busy
		for (ActorRef<DependencyWorker.Message> worker : this.dependencyWorkers) {
			if(this.actorOccupationMap.get(worker).isEmpty()) {
				//new task available
				if(this.dataprov.new_job_bool()) {
					IndexClassColumn newRef = this.dataprov.nextRef();
					this.actorColumnMap.put(worker, newRef);

					//Msg handling
					DependencyWorker.TaskMessage msg = this.dataprov.new_job(newRef);
					this.actorOccupationMap.get(worker).add(msg);
					worker.tell(msg);
				}
				//else i am not busy msg?
			}
		}
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.actorOccupationMap.put(dependencyWorker, new ArrayList<>());
			this.getContext().watch(dependencyWorker);
			//new task available
			if(this.dataprov.new_job_bool()) {
				IndexClassColumn newRef = this.dataprov.nextRef();
				this.actorColumnMap.put(dependencyWorker, newRef);

				//Msg handling
				DependencyWorker.TaskMessage msg = this.dataprov.new_job(newRef);
				this.actorOccupationMap.get(dependencyWorker).add(msg);
				dependencyWorker.tell(msg);
			}
			//else i am not busy msg?
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
		if (ind != null){
			this.resultCollector.tell(new ResultCollector.ResultMessage(Collections.singletonList(ind)));
		}
		this.resultCollector.tell(new ResultCollector.ResultMessage(Collections.singletonList(ind)));
		//TODO: Nary Deps beruecksichtigen!


		this.getContext().getLog().info("CompletionMessage ID {} :", message.id);
		return this;
	}

	private Behavior<Message> handle(RequestDataMessage message) {
		this.getContext().getLog().info("RequestDataMessage ID: {}", message.id);
		ActorRef<LargeMessageProxy.Message> receiverProxy = message.dependencyWorkerReceiverProxy;

		String[] ref;
		String[] dep;

		//reference
		if (message.refBool) { ref = null;
		} else { ref = this.fileRepresentation[message.getRefIndex().getFile()][message.getRefIndex().getColumn()];}
		//dependent
		dep = Arrays.copyOfRange(this.fileRepresentation[message.getDepIndex().getFile()][message.getDepIndex().getColumn()], message.depStart, message.depEnd);
		//data for worker
		LargeMessageProxy.LargeMessage msg;
		//TODO: da müsste eigentlich message.referencedVal und message.getDependencyVal stehen usw und ich bekomme den cast nicht hin
		msg = new DependencyWorker.tempMessage(getContext().getSelf(), message.getRefIndex(), message.getDepIndex(), ref, dep, message.id);
		//send data
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(msg, receiverProxy));
		return this;
	}


	//shutdown handling!!
	private Behavior<Message> handle(ShutdownMessage message) {return Behaviors.stopped();}

	//TODO: könnte noch in die Completion msg eingebaut werden um die zeit eines jobs zu zeigen
	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		List<DependencyWorker.TaskMessage> taskMessages = this.actorOccupationMap.remove(dependencyWorker);
		this.actorColumnMap.remove(dependencyWorker);
		this.dependencyWorkers.remove(dependencyWorker);

		//TODO: hmh wir haben noch die crashed actors, wollen wir die erneut als job aufnehmen oder nicht?
		return this;
	}
}