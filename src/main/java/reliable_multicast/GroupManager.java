package reliable_multicast;

import java.util.HashSet;
import java.util.Map;
import scala.concurrent.duration.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.Props;
import reliable_multicast.messages.AliveMsg;
import reliable_multicast.messages.CheckViewMsg;
import reliable_multicast.messages.FlushMsg;
import reliable_multicast.messages.JoinRequestMsg;
import reliable_multicast.messages.Message;
import reliable_multicast.messages.StopMulticastMsg;
import reliable_multicast.messages.ViewChangeMsg;

public class GroupManager extends EventsController {
	
	// id generator used for ID assignment to
	// nodes joining
	private int idPool;
	
	private Set<ActorRef> alivesReceived;
	private static final int ALIVE_TIMEOUT = 
			BaseParticipant.MULTICAST_INTERLEAVING / 2;
	
	private void initGroupManager(int id) {
		this.id = id;
		this.aliveProcesses
		    .addIdRefAssoc(this.id, this.getSelf());
		this.idPool = this.id + 1;
		this.alivesReceived = new HashSet<>();

		// The Group Manager is the first
		// element of the view
		HashSet<ActorRef> initialView = new HashSet<ActorRef>();
		initialView.add(this.getSelf());
		this.view = new View(0, initialView);
		this.tempView = new View(view);
		System.out.printf("%d P-%d P-%d INFO Group_manager_initiated\n",
				System.currentTimeMillis(),
				this.id,
				this.id);
		System.out.printf("%d P-%d P-%d INFO View %s\n",
				System.currentTimeMillis(),
				this.id,
				this.id,
				this.view.toString());

		this.getSelf().tell(new CheckViewMsg(), this.getSelf());
	}
	
	/**
	 * Constructor thought to be used when manual
	 * mode is wanted.
	 * @param id
	 * @param manualMode
	 * @param events
	 * @param steps
	 */
	public GroupManager(int id,
						boolean manualMode,
						Map<String, Event> events,
						Map<Integer, Set<String>> sendOrder,
						Map<Integer, Set<String>> risenOrder,
						Map<Integer, Set<String>> views) {
		super(manualMode, events, sendOrder, risenOrder, views);
		this.initGroupManager(id);
		
	}
	
	/**
	 * Constructor thought to be used when auto
	 * mode is wanted.
	 * @param id
	 */
	public GroupManager(int id) {
		super(false);
		this.initGroupManager(id);
	}
	
	public static Props props(int id,
							  boolean manualMode,
							  Map<String, Event> events,
							  Map<Integer, Set<String>> sendOrder,
							  Map<Integer, Set<String>> risenOrder,
							  Map<Integer, Set<String>> views) {
	    return Props.create(GroupManager.class,
	    		() -> new GroupManager(id, manualMode, events,
	    							   sendOrder, risenOrder,
	    							   views));
	}
	
	public static Props props(int id) {
		return Props.create(GroupManager.class,
	    		() -> new GroupManager(id));
	}
	
	public int getIdPool() {
		return idPool;
	}
	
	public Set<ActorRef> getAlivesReceived() {
		return new HashSet<>(this.alivesReceived);
	}
	
	private void onJoinRequestMsg(JoinRequestMsg request) {
		/*
		 System.out.printf("%d P-%s P-%s INFO join_request\n",
				System.currentTimeMillis(),
				this.getSelf().path().name(),
				this.getSender().path().name());
		*/
		JoinRequestMsg response = new JoinRequestMsg(this.idPool);
		this.getSender().tell(response, this.getSelf());
		
		// add a new entry to the association map
		this.aliveProcesses
			.addIdRefAssoc(this.idPool, this.getSender());
		this.idPool += 1;

		// define the new view
		// we start from the last temporary view since it's
		// the most up to date.
		Set<ActorRef> newView = new HashSet<>(this.tempView.members);
		newView.add(this.getSender());
		onViewChange(newView);
	}
	
	@Override
	protected void onReceiveMessage(Message message) {
		super.onReceiveMessage(message);
	}
	
	private void onViewChange(Set<ActorRef> newMembers) {
		// tell every member in the view to stop
		// generating new multicasts
		for (ActorRef actorRef : newMembers) {
			actorRef.tell(new StopMulticastMsg(), this.getSelf());
		}

		// Due to FIFO guarantees given by the Akka
		// framework, we are (we should) be safe not
		// send the view change before acknowledging
		// everyone has stopped sending multicasts.
		
		this.tempView = new View(this.tempView.id + 1,
				newMembers);
		System.out.printf("%d P-%d P-%d INFO change-view: %s\n",
				System.currentTimeMillis(),
				this.id,
				this.id,
				this.tempView.toString());
		ViewChangeMsg viewMsg = new ViewChangeMsg(this.tempView);
		for (ActorRef actorRef : newMembers) {
			actorRef.tell(viewMsg, this.getSelf());
		}
	}

	/*
	 * Send a message to each member in the view. If a response has not been
	 * received by some member (from a previous call to the method) then issue a
	 * view change.
	 */
	private void onCheckViewMsg(CheckViewMsg msg) {
		System.out.printf("%d P-%d P-%d INFO Checking survivors\n",
				System.currentTimeMillis(),
				this.id,
				this.id);
		if (alivesReceived.size() > 0) {
			/* here the view must be changed.
			 * New members are current members minus the ones
			 * from which the heartbeat has not been received.
			 */
			System.out.printf("%d P-%d P-%d INFO Some node crashed.\n",
					System.currentTimeMillis(),
					this.id,
					this.id);
			Set<ActorRef> newView = new HashSet<>(this.view.members);
			for (ActorRef actor : alivesReceived) {
				newView.remove(actor);
				onCrashedProcess(actor);
			}
			alivesReceived.clear();
			onViewChange(newView);
		} else {
			
			HashSet<ActorRef> participants =
					new HashSet<>(this.tempView.members);
			participants.remove(this.getSelf()); // exclude the group manager
			
			for (ActorRef participant : participants) {
				alivesReceived.add(participant);
				participant.tell(new AliveMsg(this.aliveId, this.id),
								 this.getSelf());
			}
			aliveId++;
		}
		this.getContext()
			.getSystem()
			.scheduler()
			.scheduleOnce(Duration.create(
					ALIVE_TIMEOUT / 2, TimeUnit.SECONDS),
					this.getSelf(),
					new CheckViewMsg(),
					getContext().system().dispatcher(),
					this.getSelf());
	}

	private void onAliveMsg(AliveMsg msg) {
		alivesReceived.remove(this.getSender());
		System.out.printf("%d P-%d P-%d received_alive_message %s\n",
				System.currentTimeMillis(),
				this.id,
				msg.senderID,
				msg.toString());
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(JoinRequestMsg.class, this::onJoinRequestMsg)
				.match(StopMulticastMsg.class, this::onStopMulticast)
				.match(ViewChangeMsg.class, this::onViewChangeMsg)
				.match(FlushMsg.class, this::onFlushMsg)
				.match(SendMulticastMsg.class, this::onSendMulticastMsg)
				.match(Message.class, this::onReceiveMessage)
				.match(CheckViewMsg.class, this::onCheckViewMsg)
				.match(AliveMsg.class, this::onAliveMsg)
				// handle (receiving) the step message defined in
				// the EventsController
				.match(SendStepMsg.class, this::onSendStepMsg)				
				.build();
	}
}
