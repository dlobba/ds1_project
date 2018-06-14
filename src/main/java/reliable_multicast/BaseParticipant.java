package reliable_multicast;

import java.util.HashSet;
import java.util.Set;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import reliable_multicast.messages.FlushMsg;
import reliable_multicast.messages.Message;
import reliable_multicast.messages.StopMulticastMsg;
import reliable_multicast.messages.ViewChangeMsg;

public class BaseParticipant extends AbstractActor {

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

	protected void onStopMulticast(StopMulticastMsg stopMsg) {
		this.canSend = false;
		System.out.printf("Participant %d cannot send multicasts anymore.\n", this.id);
	}
	
	protected void onViewChangeMsg(ViewChangeMsg viewChange) {
		System.out.printf("Participant %d starts view-change phases\n",
				this.id);
		this.flushesReceived.clear();
		this.tempView = new View(viewChange.view);
		for (Message message : messagesUnstable) {
			for (ActorRef member : this.tempView.members) {
				member.tell(message, this.getSelf());
			}
		}
		for (ActorRef member : this.tempView.members) {
			member.tell(new FlushMsg(), this.getSelf());
		}
	}

	protected void onFlushMsg(FlushMsg flushMsg) {
		this.flushesReceived.add(this.getSender());
		System.out.printf("Part. %s received flush from Part. %s.\n",
				this.getSelf().path().name(),
				this.getSender().path().name());
		// if this is true then every operational
		// node has received all the unstable messages
		if (this.flushesReceived.containsAll(this.tempView.members)) {
			this.messagesUnstable.clear();
			this.view = new View(tempView);
			this.canSend = true;
			System.out.printf("Participant %d has installed %s and can now send.\n",
					this.id, this.view.toString());
		}
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StopMulticastMsg.class, this::onStopMulticast)
				.match(ViewChangeMsg.class, this::onViewChangeMsg)
				.match(FlushMsg.class, this::onFlushMsg)
				//.match(Message.class, this::onReceiveMessage)
				.build();
	}
}
