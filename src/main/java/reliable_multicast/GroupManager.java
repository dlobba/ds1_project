package reliable_multicast;
import java.util.HashSet;
import java.util.Set;

import akka.actor.ActorRef;
import akka.actor.Props;
import reliable_multicast.messages.CrashMsg;
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
	
	public GroupManager(int id, boolean manualMode) {
		super(manualMode);
		this.id = id;
		this.idPool = 1;
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
	}
	
	public GroupManager(int id) {
		this(id, false);
	}
	
	public static Props props(int id, boolean manualMode) {
	    return Props.create(GroupManager.class,
	    		() -> new GroupManager(id, manualMode));
	}
	
	public static Props props(int id) {
	    return props(id, false);
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
		this.addIdRefAssoc(this.idPool, this.getSender());
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
		this.triggerEvent(message);
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
	
//	/*
//	 * Send a message to each member in the view.
//	 * If a response has not been received by some
//	 * member (from a previous call to the method)
//	 * then issue a view change.
//	 */
//	private void onCheckViewMsg(CheckViewMsg msg) {
//		System.out.printf("Start checking survivors!\n");
//		if (alivesReceived.size() > 0) {
//			System.out.printf("Some node crashed!\n");
//		} else {
//			for (ActorRef member : view.members) {
//				member.tell(new AliveMsg(), this.getSelf());
//			}
//		}
//		this.getContext().getSystem().scheduler()
//		.scheduleOnce(Duration.create(ALIVE_TIMEOUT / 2, TimeUnit.SECONDS),
//				this.getSelf(),
//				new CheckViewMsg(),
//				getContext().system().dispatcher(),
//				this.getSelf());
//	}
	
	/*
	 * this is just temporary, when a node crashes it sends
	 * a crashed message to the gm who issues a view change
	 */
	private void onCrashedMessage(CrashMsg msg) {
		Set<ActorRef> newView = new HashSet<>(this.tempView.members);
		newView.remove(this.getSender());
		this.removeIdRefEntry(
				this.getIdByActor(
						this.getSender()));
		onViewChange(newView);
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(JoinRequestMsg.class,  this::onJoinRequestMsg)
				.match(StopMulticastMsg.class, this::onStopMulticast)
				.match(ViewChangeMsg.class, this::onViewChangeMsg)
				.match(FlushMsg.class, this::onFlushMsg)
				.match(Message.class, this::onReceiveMessage)
//				.match(CheckViewMsg.class,  this::onCheckViewMsg)
				// temporary: crashes should be automatically
				// notified after a timeout
				.match(CrashMsg.class, this::onCrashedMessage)
				.build();
	}
}
