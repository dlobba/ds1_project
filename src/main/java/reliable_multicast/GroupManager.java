package reliable_multicast;
import java.util.HashSet;
import java.util.Set;

import akka.actor.ActorRef;
import akka.actor.Props;
import reliable_multicast.messages.FlushMsg;
import reliable_multicast.messages.JoinRequestMsg;
import reliable_multicast.messages.StopMulticastMsg;
import reliable_multicast.messages.ViewChangeMsg;

public class GroupManager extends BaseParticipant{
	
	// id generator used for ID assignment to
	// nodes joining
	private int idPool;
	private Set<ActorRef> alivesReceived;
	private static final int ALIVE_TIMEOUT = 10;
	
	public GroupManager(int id) {
		super();
		this.id = id;
		this.idPool = 1;
		this.alivesReceived = new HashSet<>();
		
		// The Group Manager is the first
		// element of the view
		HashSet<ActorRef> initialView = new HashSet<ActorRef>();
		initialView.add(this.getSelf());
		this.view = new View(0, initialView);
		
		System.out.printf("Group manager initiated, %s\n",
				this.view.toString());
	}
	
	public static Props props(int id) {
	    return Props.create(GroupManager.class, () -> new GroupManager(id));
	}
	
	private void onJoinRequestMsg(JoinRequestMsg request) {
		System.out.printf("Received join request from %s\n",
				this.getSender().path().name());
		JoinRequestMsg response = new JoinRequestMsg(this.idPool);
		this.getSender().tell(response, this.getSelf());
		this.idPool += 1;
		
		// define the new view
		Set<ActorRef> newView = new HashSet<>(this.view.members);
		newView.add(this.getSender());
		onViewChange(newView);
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
		
		this.tempView = new View(this.view.id + 1,
				newMembers);
		System.out.printf("Group manager issues a view change, members: %s\n",
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
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(JoinRequestMsg.class,  this::onJoinRequestMsg)
				.match(StopMulticastMsg.class, this::onStopMulticast)
				.match(ViewChangeMsg.class, this::onViewChangeMsg)
				.match(FlushMsg.class, this::onFlushMsg)
//				.match(CheckViewMsg.class,  this::onCheckViewMsg)
				.build();
	}
}