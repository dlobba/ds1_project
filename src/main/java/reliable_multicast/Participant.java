package reliable_multicast;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.Props;
import reliable_multicast.messages.*;
import reliable_multicast.messages.events_messages.MulticastCrashMsg;
import reliable_multicast.messages.events_messages.ReceivingCrashMsg;
import scala.concurrent.duration.Duration;

public class Participant extends BaseParticipant {

	public static class CheckGmAliveMsg implements Serializable {};
	
	protected ActorRef groupManager;
	protected boolean crashed;
  
	protected boolean receiveMessageAndCrash;
	protected boolean receiveViewChangeAndCrash;
	private String ignoreMessageLabel;
	private boolean isGmAlive;
	
	private static final int ALIVE_TIMEOUT = 
			BaseParticipant.MULTICAST_INTERLEAVING / 2;
	
	/*
	 * This will be called in the constructor by issuing
	 * the super() method.
	 * 
	 * @see reliable_multicast.BaseParticipant#resetParticipant()
	 */
	@Override
	protected void resetParticipant() {
		super.resetParticipant();
		this.receiveMessageAndCrash = false;
		this.receiveViewChangeAndCrash = false;
	}

	// Constructors
	public Participant(ActorRef groupManager, boolean manualMode) {
		super(manualMode);
		this.groupManager = groupManager;
		this.crashed = false;
		this.isGmAlive = false;
		this.groupManager.tell(new JoinRequestMsg(),
							   this.getSelf());
	}
	
	public Participant(ActorRef groupManager) {
		this(groupManager, false);
	}
	
	public Participant(String groupManagerPath, boolean manualMode) {
		super(manualMode);
		this.crashed = false;
		this.isGmAlive = false;
		this.groupManager = null;
		getContext().actorSelection(groupManagerPath)
					.tell(new JoinRequestMsg(),
						  this.getSelf());
	}
	
	public Participant(String groupManagerPath) {
		this(groupManagerPath, false);
	}
	
	public static Props props(ActorRef groupManager, boolean manualMode) {
		return Props.create(Participant.class,
				() -> new Participant(groupManager, manualMode));
	}
	
	public static Props props(ActorRef groupManager) {
		return props(groupManager, false);
	}
	
	public static Props props(String groupManagerPath, boolean manualMode) {
		return Props.create(Participant.class,
				() -> new Participant(groupManagerPath, manualMode));
	}
	
	public static Props props(String groupmanagerPath) {
		return props(groupmanagerPath, false);
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
		this.groupManager = this.getSender();
		System.out.printf("%d P-%d P-%s JOIN-ASSOC\n",
				System.currentTimeMillis(),
				this.id,
				this.getSelf().path().name());
		
		this.getSelf().tell(new CheckGmAliveMsg(), this.getSelf());
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
		
		if (this.receiveViewChangeAndCrash) {
			this.receiveViewChangeAndCrash = false;
			this.crashAfterViewChange(viewChange);
		}
		else
			super.onViewChangeMsg(viewChange);
	}
	
	/**
	 * Replicate the behavior of the normal view-change method,
	 * but instead of sending the FLUSH messages the node crashes.
	 * @param viewChange
	 */
	protected void crashAfterViewChange(ViewChangeMsg viewChange) {
		System.out.printf("%d P-%d P-%d INFO started_view-change V%d\n",
				System.currentTimeMillis(),
				this.id,
				this.id,
				viewChange.id);
		this.flushesReceived.clear();
		this.tempView = new View(viewChange.id,
								 viewChange.members);
		for (Message message : messagesUnstable) {
			for (ActorRef member : this.tempView.members) {
				member.tell(message, this.getSelf());
			}
		}
		this.crash();
		// FLUSHES are not sent
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

		if (this.receiveMessageAndCrash &&
			!this.ignoreMessageLabel.equals(message.getLabel())) {
			// remove the flag (so when the node revives
			// it won't crash suddenly).
			// Then let the node crash.
			this.receiveMessageAndCrash = false;
			this.crash();
		}
	}
	
	private void crash() {
		System.out.printf("%d P-%d P-%d CRASHED\n",
				System.currentTimeMillis(),
				this.id,
				this.id);
		this.resetParticipant();
		this.crashed = true;
		this.canSend = false;
	}
	
	// EXT: external behavior message handlers --
	
	/**
	 * This message was used before having the
	 * event-handler system based on config.
	 * 
	 * Now the message is kept to let
	 * the node crash from the outside and without
	 * a config approach.
	 * 
	 * @param crashMsg
	 */
	private void onCrashMsg(CrashMsg crashMsg) {
		this.crash();
	}

    private void onAliveMsg(AliveMsg aliveMsg) {			
		if (this.crashed)
			return;
		this.getSender()
			.tell(
				new AliveMsg(this.aliveId, this.id),
				this.getSelf());
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
				this.tempView.id,
				false);
		this.multicastId += 1;
		
		for (ActorRef member : this.view.members) {
			member.tell(message, this.getSelf());
		}
		// Do not send stable messages.
		// Crash instead
		this.crash();
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

		Set<ActorRef> members = this.view.members;
		if (members.size() < 3) {
			System.out.printf("%d P-%d P-%s WARNING: too few view members."
					+ " Two participants and the group manager are required."
					+ " Crash denied. Multicast aborted. \n",
					System.currentTimeMillis(),
					this.id,
					this.id);
			return;
		}

		/*
		 * Avoid choosing the group manager
		 * or self as receiver.
		 */
		Iterator<ActorRef> memberIterator = members.iterator();
		ActorRef receiver = null;
		boolean found = false;
		while (memberIterator.hasNext() && !found) {
			receiver = memberIterator.next();
			
			if (!(receiver.equals(this.getSelf()) ||
				  receiver.equals(this.groupManager)))
				found = true;
		}
		System.out.println(receiver.toString());
		Message message = new Message(this.id,
				this.multicastId,
				this.tempView.id,
				false);
		this.multicastId += 1;
		receiver.tell(message, this.getSelf());

		// let the sender crash
		this.crash();
	}
	
	protected void onSendMutlicastCrashMsg(MulticastCrashMsg crashMsg) {
		switch (crashMsg.type) {
		case MULTICAST_N_CRASH:
			System.out
			  .printf("%d P-%d P-%s INFO process will multicast then crash\n",
					  System.currentTimeMillis(),
					  this.id,
					  this.id);
			this.multicastAndCrash();
			break;
		case MULTICAST_ONE_N_CRASH:
			System.out
			  .printf("%d P-%d P-%s INFO process will multicast to one" +
					  " particpant then crash\n",
					  System.currentTimeMillis(),
					  this.id,
					  this.id);
			this.multicastOneAndCrash();
			break;
		}
	}
	
	protected void onReceivingMulticastCrashMsg(ReceivingCrashMsg crashMsg) {
		this.ignoreMessageLabel = crashMsg.eventLabel;
		switch (crashMsg.type) {
		case RECEIVE_MULTICAST_N_CRASH:
			this.receiveMessageAndCrash = true;
			System.out
				  .printf("%d P-%d P-%s INFO process set to crash on" +
						  " next message receiving. \n",
						  System.currentTimeMillis(),
						  this.id,
						  this.id);
			break;
		case RECEIVE_VIEW_N_CRASH:
			this.receiveViewChangeAndCrash = true;
			System.out
			  .printf("%d P-%d P-%s INFO process set to crash on" +
					  " next view-change message receiving. \n",
					  System.currentTimeMillis(),
					  this.id,
					  this.id);
			break;
		}
	}
	
	private void onCheckGmAliveMsg(CheckGmAliveMsg msg) {
		if(crashed)
			return;

		System.out.printf("%d P-%d P-%d INFO Checking Group Manager\n",
				System.currentTimeMillis(),
				this.id,
				this.id);
		
		if(!isGmAlive) {
			System.out
				  .printf("%d P-%d P-%d INFO Group manager Unreachable." +
						  " Exiting...\n",
						  System.currentTimeMillis(),
						  this.id,
						  this.id);
			this.getContext().stop(this.getSelf());
			this.getContext().system().terminate();
		} else {
			isGmAlive = false;
			groupManager.tell(new GmAliveMsg(), this.getSelf());
			this.getContext()
				.getSystem()
				.scheduler()
				.scheduleOnce(Duration.create(
						ALIVE_TIMEOUT / 2, TimeUnit.SECONDS),
						this.getSelf(),
						new CheckGmAliveMsg(),
						getContext().system().dispatcher(),
						this.getSelf());
		}
	}
	
	private void onGmAliveMsg(GmAliveMsg msg) {
		if (crashed)
			return;
		// we cannot process the message if the
		// node is crashed
		isGmAlive = true;
		System.out.printf("%d P-%d P-%d received_gm_alive_message\n",
				System.currentTimeMillis(),
				this.id,
				this.id);
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
				.match(MulticastCrashMsg.class,
						this::onSendMutlicastCrashMsg)
				.match(ReceivingCrashMsg.class,
						this::onReceivingMulticastCrashMsg)		
				.match(AliveMsg.class, this::onAliveMsg)
				.match(CheckGmAliveMsg.class, this::onCheckGmAliveMsg)
				.match(GmAliveMsg.class, this::onGmAliveMsg)
				.build();
	}
}
