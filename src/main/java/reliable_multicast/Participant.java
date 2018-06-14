package reliable_multicast;
import akka.actor.ActorRef;
import akka.actor.Props;
import reliable_multicast.messages.*;

public class Participant extends BaseParticipant {
	
	protected ActorRef groupManager;
	protected boolean crashedMode;
	
	// Constructors
	public Participant(ActorRef groupManager) {
		super();
		this.groupManager = groupManager;
		this.crashedMode = false;
		
		this.groupManager.tell(new JoinRequestMsg(), this.getSelf());
	}
	
	public static Props props(ActorRef groupmanager) {
		return Props.create(Participant.class, () -> new Participant(groupmanager));
	}
	
	//--------------------------------

	/* 
	 * When a participant receives a JoinRequestMsg it's
	 * only when the Group Manager answers to the request
	 * with the ID for the participant.
	 */
	private void onJoinMsg(JoinRequestMsg joinResponse) {
		this.id = joinResponse.idAssigned;
		System.out.printf("Participant %s has been given id %d.\n", 
				this.getSelf().path().name(),
				this.id);
	}
	
	
//	
//	private void onReceiveMessage(Message message) {
//		System.out.print("Participant %d received message $d", this.id, message.id);
//		// deliver here
//		System.out.print("Participant %d delivered message $d", this.id, message.id);
//		this.messagesUnstable.add(message);
//	}
//	
//	private void performMulticast() {
//		for (ActorRef member : view) {
//			this.getSelf().tell(new Message(this.id + "-" + this.multicastId),
//					member);
//		}
//		this.multicastId += 1;
//
//		// this node cannot send message
//		// until this one is completed
//		this.canSend = false;
//		
//		// send stable
//		
//	}
//	
//	private void onNextMulticast() {
//		if (canSend)
//			performMulticast();
//	}
	
	
	
//	private void onAliveMsg(AliveMsg aliveMsg) {
//		this.getSelf().tell(new AliveMsg(), this.getSender());
//	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(JoinRequestMsg.class, this::onJoinMsg)
				.match(StopMulticastMsg.class, this::onStopMulticast)
				.match(ViewChangeMsg.class, this::onViewChangeMsg)
				.match(FlushMsg.class, this::onFlushMsg)
				//.match(Message.class, this::onReceiveMessage)
				//.match(AliveMsg.class, this::onAliveMsg)
				.build();
	}
}
