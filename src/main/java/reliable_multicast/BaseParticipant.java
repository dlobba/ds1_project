package reliable_multicast;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import reliable_multicast.messages.FlushMsg;
import reliable_multicast.messages.Message;
import reliable_multicast.messages.StopMulticastMsg;
import reliable_multicast.messages.ViewChangeMsg;
import scala.concurrent.duration.Duration;


public class BaseParticipant extends AbstractActor {

	// --- Messages for internal behavior ---
	public class SendMulticastMsg implements Serializable {};
	
	// --------------------------------------
	
	public static final int MULTICAST_INTERLEAVING = 10;
	protected int id;
	protected int multicastId;
	// the set of actors that are seen by this node
	protected View view;
	
	// temporary view established in the all-to-all
	// phase
	protected View tempView;
	protected boolean canSend;
	protected Set<ActorRef> flushesReceived;
	protected Set<Message> messagesUnstable;
	
	public BaseParticipant() {
		super();
		this.id = -1;
		this.multicastId = 0;
		this.view = new View(-1);
		this.tempView = new View(-1);
		this.canSend = false;
		this.messagesUnstable = new HashSet<>();
		this.flushesReceived = new HashSet<>();
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
				viewChange.view.id);
		this.flushesReceived.clear();
		this.tempView = new View(viewChange.view);
		for (Message message : messagesUnstable) {
			for (ActorRef member : this.tempView.members) {
				member.tell(message, this.getSelf());
			}
		}
		// FLUSH messages
		for (ActorRef member : this.tempView.members) {
			member.tell(new FlushMsg(this.id, this.tempView.id),
					this.getSelf());
		}
	}

	protected void onFlushMsg(FlushMsg flushMsg) {
		/*
		 * if the flush is for a previous view change
		 * then just ignore it. The flush set
		 * has already been cleared from flushes related
		 * the previous view change (checkout onViewChangeMsg).
		 */
		if (flushMsg.viewID < this.tempView.id)
			return;
			
		this.flushesReceived.add(this.getSender());
		System.out.printf("%d P-%d P-%d received_flush V%d\n",
				System.currentTimeMillis(),
				this.id,
				flushMsg.senderID,
				flushMsg.viewID);
		// if this is true then every operational
		// node has received all the unstable messages
		if (this.flushesReceived.containsAll(this.tempView.members)) {
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
	
	private void scheduleMulticast() {
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
