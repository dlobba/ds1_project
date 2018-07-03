package reliable_multicast;
import akka.actor.ActorRef;
import akka.actor.Props;
import reliable_multicast.messages.*;

public class Participant extends BaseParticipant {
	
	protected ActorRef groupManager;
	protected boolean crashed;
	
	// Constructors
	public Participant(ActorRef groupManager) {
		super();
		this.groupManager = groupManager;
		this.crashed = false;
		
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
		if (this.crashed)
			return;
		this.id = joinResponse.idAssigned;
		System.out.printf("%d P-%d P-%s JOIN-ASSOC\n",
				System.currentTimeMillis(),
				this.id,
				this.getSelf().path().name());
	}
	
	private void onCrashMsg(CrashMsg crashMsg) {
		this.crashed = true;
		this.canSend = false;
		System.out.printf("%d P-%d P-%s CRASHED\n",
				System.currentTimeMillis(),
				this.id,
				this.id);
		this.groupManager.tell(new CrashMsg(), this.getSelf());
	}
	
	@Override
	protected void onStopMulticast(StopMulticastMsg stopMsg) {
		if (this.crashed)
			return;
		super.onStopMulticast(stopMsg);
	}

	@Override
	protected void onViewChangeMsg(ViewChangeMsg viewChange) {
		if (this.crashed)
			return;
		super.onViewChangeMsg(viewChange);
	}

	@Override
	protected void onFlushMsg(FlushMsg flushMsg) {
		if (this.crashed)
			return;
		super.onFlushMsg(flushMsg);
	}

	@Override
	protected void onReceiveMessage(Message message) {
		if (this.crashed)
			return;
		super.onReceiveMessage(message);
	}

	private void onAliveMsg(AliveMsg aliveMsg) {			
		if (this.crashed)
			return;
		this.getSender()
			.tell(
				new AliveMsg(this.aliveId, this.id),
				this.getSelf());
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(JoinRequestMsg.class, this::onJoinMsg)
				.match(StopMulticastMsg.class, this::onStopMulticast)
				.match(ViewChangeMsg.class, this::onViewChangeMsg)
				.match(FlushMsg.class, this::onFlushMsg)
				.match(Message.class, this::onReceiveMessage)
				.match(SendMulticastMsg.class, this::onSendMulticastMsg)
				.match(CrashMsg.class, this::onCrashMsg)
				.match(AliveMsg.class, this::onAliveMsg)
				.build();
	}
}
