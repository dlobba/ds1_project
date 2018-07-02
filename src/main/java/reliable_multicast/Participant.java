package reliable_multicast;
import java.io.Serializable;

import akka.actor.ActorRef;
import akka.actor.Props;
import reliable_multicast.messages.*;

public class Participant extends BaseParticipant {
	
	public enum CrashType {
		MULTICAST_N_CRASH,
		MULTICAST_ONE_N_CRASH,
		RECEIVE_VIEW_N_CRASH,
		RECEIVE_MULTICAST_N_CRASH
	}
	
	public class SendCrashMsg implements Serializable {
		final CrashType type;
		public SendCrashMsg(CrashType type) {
			this.type = type;
		}
	};
	
	protected ActorRef groupManager;
	protected boolean crashed;
	protected boolean doAndCrash;
	
	// Constructors
	public Participant(ActorRef groupManager) {
		super();
		this.groupManager = groupManager;
		this.crashed = false;
		this.doAndCrash = false;
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
	
//	private void onAliveMsg(AliveMsg aliveMsg) {
//		this.getSelf().tell(new AliveMsg(), this.getSender());
//	}
	
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
	
	// EXT: external behavior message handlers --
	
	private void onCrashMsg(CrashMsg crashMsg) {
		this.crashed = true;
		this.canSend = false;
		System.out.printf("%d P-%d P-%s CRASHED\n",
				System.currentTimeMillis(),
				this.id,
				this.id);
		this.groupManager.tell(new CrashMsg(), this.getSelf());
	}
	
	/**
	 * Turn off crashed mode and ask the group
	 * manager to join.
	 * 
	 * @param reviveMsg
	 */
	private void onReviveMsg(ReviveMsg reviveMsg) {
		this.crashed = false;
		this.groupManager.tell(new JoinRequestMsg(), this.getSelf());
	}
	
	// EXT --------------------------------------

	// implementing sending and receiving -------
		// variants with crashes --------------------
		
		private void multicastAndCrash() {
			if (! this.canSend)
				return;
			
			// this node cannot send message
			// until this one is completed
			this.canSend = false;

			Message message = new Message(this.id,
					this.multicastId,
					false);
			this.multicastId += 1;
			
			for (ActorRef member : this.view.members) {
				member.tell(message, this.getSelf());
			}
			// Do not send stable messages.
			// Crash instead
			this.getSelf().tell(new CrashMsg(), this.getSelf());
		}
		
		/**
		 * While performing a multicast, the message
		 * is effectively sent just to one other actor, then
		 * the sender crashes.
		 */
		private void multicastOneAndCrash() {
			if (! this.canSend)
				return;
			// this node cannot send messages
			// until this one is completed
			this.canSend = false;

			ActorRef[] members = this.view.members.toArray(new ActorRef[0]);
			if (members.length < 2) {
				System.out.printf("%d P-%d P-%s WARNING: too few view members."
						+ "Crash denied. Multicast aborted. \n",
						System.currentTimeMillis(),
						this.id,
						this.id);
				return;
			}
			
			Message message = new Message(this.id,
					this.multicastId,
					false);
			this.multicastId += 1;
			ActorRef receiver = members[0];
			
			// manage the unlucky case in which
			// the only node seeing the message is
			// the crashing node...
			if (receiver.equals(this.getSelf()))
				receiver = members[1];
			
			receiver.tell(message, this.getSelf());
			
			// let the sender crash
			this.getSelf().tell(new CrashMsg(), this.getSelf());
		}
		
		protected void onSendCrashMsg(SendCrashMsg crashMsg) {
			switch (crashMsg.type) {
			case MULTICAST_N_CRASH:
				this.multicastAndCrash();
				break;
			case MULTICAST_ONE_N_CRASH:
				this.multicastOneAndCrash();
				break;
			case RECEIVE_MULTICAST_N_CRASH:
				this.doAndCrash = true; 
				break;
			case RECEIVE_VIEW_N_CRASH:
				this.doAndCrash = true;
				break;
			}
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
				.match(ReviveMsg.class, this::onReviveMsg)
				.match(SendCrashMsg.class, this::onSendCrashMsg)
				//.match(AliveMsg.class, this::onAliveMsg)
				.build();
	}
}
