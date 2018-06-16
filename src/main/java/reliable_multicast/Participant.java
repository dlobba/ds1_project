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
		System.out.printf("%d P-%d P-%s JOIN-ASSOC\n",
				System.currentTimeMillis(),
				this.id,
				this.getSelf().path().name());
	}
	
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
				.match(Message.class, this::onReceiveMessage)
				.match(SendMulticastMsg.class, this::onSendMulticastMsg)
				//.match(AliveMsg.class, this::onAliveMsg)
				.build();
	}
}
