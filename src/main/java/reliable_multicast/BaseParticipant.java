package reliable_multicast;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import reliable_multicast.messages.FlushMsg;
import reliable_multicast.messages.Message;
import reliable_multicast.messages.StopMulticastMsg;
import reliable_multicast.messages.ViewChangeMsg;
import reliable_multicast.utils.Config;
import scala.concurrent.duration.Duration;


public class BaseParticipant extends AbstractActor {

	// --- Messages for internal behavior ---
	public static class SendMulticastMsg implements Serializable {};
	
	// --------------------------------------
	
	public static final int MULTICAST_INTERLEAVING = 10;
	protected int id;
	protected int multicastId;
	protected int aliveId;
	// the set of actors that are seen by this node
	protected View view;
	protected boolean manualMode;
	
	// temporary view established in the all-to-all
	// phase
	protected View tempView;
	protected boolean canSend;
	protected Set<FlushMsg> flushesReceived;
	protected Set<Message> messagesUnstable;
	
	protected void resetParticipant() {
		this.id = -1;
		this.multicastId = 0;
		this.aliveId = 0;
		this.canSend = false;
		this.view = new View(-1);
		this.tempView = new View(-1);
		this.messagesUnstable = new HashSet<>();
		this.flushesReceived = new HashSet<>();
	}
	
	public BaseParticipant(boolean manualMode) {
		super();
		this.resetParticipant();
		this.manualMode = manualMode;
	}
	
	public BaseParticipant(Config config) {
		this(config.isManual_mode());
	}
	
	public BaseParticipant() {
		this(false);
	}
	
	private void removeOldFlushes(int currentView) {
		Iterator<FlushMsg> msgIterator =
				this.flushesReceived.iterator();
		FlushMsg flushMsg;
		while (msgIterator.hasNext()) {
			flushMsg = msgIterator.next();
			if (flushMsg.viewID < currentView)
				msgIterator.remove();
		}
	}
	
	protected Set<ActorRef> getFlushSenders(int currentView) {
		Set<ActorRef> senders = new HashSet<>();
		Iterator<FlushMsg> flushMsgIterator =
				this.flushesReceived.iterator();
		FlushMsg flushMsg;
		while (flushMsgIterator.hasNext()) {
			flushMsg = flushMsgIterator.next();
			if (flushMsg.viewID == currentView)
				senders.add(flushMsg.sender);
		}
		return senders;
	}
	
	public int getId() {
		return id;
	}
	
	public int getMulticastId() {
		return multicastId;
	}
	
	public View getView() {
		return new View(view);
	}
	
	public View getTempView() {
		return new View(tempView);
	}
	
	protected void onStopMulticast(StopMulticastMsg stopMsg) {
		this.canSend = false;
		System.out.printf("%d P-%d P-%d INFO stopped_multicasting\n",
				System.currentTimeMillis(),
				this.id,
				this.id);
	}
	
	protected void onViewChangeMsg(ViewChangeMsg viewChange) {
		System.out.printf("%d P-%d P-%d INFO started_view-change V%d\n",
				System.currentTimeMillis(),
				this.id,
				this.id,
				viewChange.id);
		//this.flushesReceived.clear();
		this.tempView = new View(viewChange.id,
								 viewChange.members);
		this.removeOldFlushes(this.tempView.id);
		for (Message message : messagesUnstable) {
			for (ActorRef member : this.tempView.members) {
				member.tell(message, this.getSelf());
			}
		}
		// FLUSH messages
		for (ActorRef member : this.tempView.members) {
			member.tell(new FlushMsg(this.id,
									 this.tempView.id,
									 this.getSelf()),
					this.getSelf());
		}
	}

	protected void onFlushMsg(FlushMsg flushMsg) {
		/*
		 * if the flush is for a previous view change
		 * then just ignore it. 
		 */
		if (flushMsg.viewID < this.tempView.id)
			return;
			
		this.flushesReceived.add(flushMsg);
		System.out.printf("%d P-%d P-%d received_flush V%d\n",
				System.currentTimeMillis(),
				this.id,
				flushMsg.senderID,
				flushMsg.viewID);
		// if this is true then every operational
		// node has received all the unstable messages
		if (this.tempView.id == flushMsg.viewID &&
			this.getFlushSenders(this.tempView.id)
				.containsAll(this.tempView.members)) {
			// deliver all mesages up to current view
			this.deliverAllMessages();
			this.view = new View(tempView);
			System.out.printf("%d P-%d P-%d installed_view %s\n",
					System.currentTimeMillis(),
					this.id,
					this.id,
					this.view.toString());
			System.out.printf("%d P-%d P-%d INFO can_send\n",
					System.currentTimeMillis(),
					this.id,
					this.id);
			
			// resume multicasting
			this.canSend = true;
			this.scheduleMulticast();
		}
	}
	
	private void deliverAllMessages() {
		this.messagesUnstable.clear();
	}
	
	private void deliverMessage(Message message) {
		this.messagesUnstable.remove(message);
	}
	
	protected void onSendMulticastMsg(SendMulticastMsg message) {
		this.scheduleMulticast();
		this.multicast();
	}
	
	/**
	 * Schedule a new multicast.
	 * Be careful that here the bahavior will differ
	 * between EventsController and normal Participants.
	 * 
	 * In the case of a participant, the scheduling
	 * should be ignore if in manual mode.
	 * 
	 * While in the case of the EventController the
	 * scheduling represents a step of batch mutlicasts.
	 */
	protected void scheduleMulticast() {
		/* if in manual mode, multicasts
		 * are not sent automatically, so block
		 * the scheduling.
		 */
		if (this.manualMode)
			return;
		int time = new Random().nextInt(MULTICAST_INTERLEAVING);
		
		this.getContext().getSystem().scheduler()
			.scheduleOnce(Duration.create(time,
					TimeUnit.SECONDS),
					this.getSelf(),
					new SendMulticastMsg(),
					getContext().system().dispatcher(),
					this.getSelf());
	}
	
	private void multicast() {
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
		// STABLE messages
		message = new Message(message, true);
		for (ActorRef member : this.view.members) {
			member.tell(message, this.getSelf());
		}
		this.canSend = true;
	}
	
	protected void onReceiveMessage(Message message) {
		
		/* if the sender is not in the view, then
		 * do not accept the message
		 */
		if (!this.tempView.members.contains(this.getSender()))
			return;
		
		if (!message.stable) {
			System.out.printf("%d P-%d P-%d received_message %s\n",
					System.currentTimeMillis(),
					this.id,
					message.senderID,
					message.toString());
			this.messagesUnstable.add(message);
		} else {
			System.out.printf("%d P-%d P-%d STABLE %s\n",
					System.currentTimeMillis(),
					this.id,
					message.senderID,
					message.toString());
			this.deliverMessage(message);
		}
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StopMulticastMsg.class, this::onStopMulticast)
				.match(ViewChangeMsg.class, this::onViewChangeMsg)
				.match(FlushMsg.class, this::onFlushMsg)
				.match(Message.class, this::onReceiveMessage)
				.match(SendMulticastMsg.class, this::onSendMulticastMsg)
				.build();
	}
}
