package reliable_multicast;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import akka.actor.ActorRef;
import reliable_multicast.messages.Message;
import reliable_multicast.messages.crash_messages.*;
import reliable_multicast.messages.crash_messages.MulticastCrashMsg.MutlicastCrashType;
import reliable_multicast.messages.crash_messages.ReceivingCrashMsg.ReceivingCrashType;

public abstract class EventsController extends BaseParticipant {
	
	public enum Event {
		MULTICAST_ONE_N_CRASH,
		MULTICAST_N_CRASH,
		RECEIVE_MESSAGE_N_CRASH,
		RECEIVE_VIEW_N_CRASH
	}
	private int step;
	private final Map<Integer, Event> events;
	private Map<Integer, ActorRef> idRefMap;
	private Map<Integer, Set<Integer>> steps;
	
	public EventsController(boolean manualMode) {
		super(manualMode);
		this.step = 0;
		this.events = new HashMap<>();
		this.idRefMap = new HashMap<>();
		this.steps = new HashMap<>();
	}
	
	public EventsController() {
		this(false);
	}
	
	/**
	 * This defines a step by the event
	 * controller.
	 * 
	 * If in manual mode, the set of senders
	 * associated in this step is picked and
	 * the controller sends a message to these
	 * senders that allow them to call a multicast
	 */
	@Override
	protected void scheduleMulticast() {
		// if in auto mode, acts as normal
		if (!this.manualMode) {
			super.scheduleMulticast();
			return;
		}
		this.step += 1;
		HashSet<Integer> sendersIds = this.getSendersInStep(step);
		this.steps.remove(this.step);
		
		for (Integer senderId : sendersIds) {
			this.getActorById(senderId)
				.tell(new SendMulticastMsg(), this.getSelf());
		}
	}

	/**
	 * Check whether the sender Id in the message
	 * has an event associated. If this is the case
	 * then generate the associated message and send it
	 * to the correct receiver (as of now it's the actor
	 * associated with the sender ID).
	 * 
	 * @param msg
	 */
	protected void triggerEvent(Message msg) {
		Event event = this.events.get(msg.senderID);
		if (event == null)
			return;
		ActorRef sender = this.getActorById(msg.senderID);
		CrashMessage crashMsg = null;
		switch (event) {
		case MULTICAST_N_CRASH:
			crashMsg = new MulticastCrashMsg(
					MutlicastCrashType.MULTICAST_N_CRASH);
			break;
		case MULTICAST_ONE_N_CRASH:
			crashMsg = new MulticastCrashMsg(
					MutlicastCrashType.MULTICAST_ONE_N_CRASH);
			break;
		case RECEIVE_MESSAGE_N_CRASH:
			crashMsg = new ReceivingCrashMsg(
					ReceivingCrashType.RECEIVE_MULTICAST_N_CRASH);
			break;
		case RECEIVE_VIEW_N_CRASH:
			crashMsg = new ReceivingCrashMsg(
					ReceivingCrashType.RECEIVE_VIEW_N_CRASH);
			break;
		}
		sender.tell(crashMsg, this.getSelf());
	}
	
	public void addStep(Integer stepNumber) {
		this.steps.put(stepNumber, new HashSet<>());
	}
	
	public boolean addSender(Integer stepNumber, Integer senderId) {
		Set<Integer> senders = this.steps.get(stepNumber);
		if (senders == null)
			return false;
		return senders.add(senderId);
	}
	
	public HashSet<Integer> getSendersInStep(Integer step) {
		return new HashSet<Integer>(this.steps.get(step));
	}
	
	/**
	 * An element can be added iif it's unique
	 * both to the keys set and to the values set.
	 * 
	 * The association must be bijective.
	 * 
	 * @param id
	 * @param actor
	 * @return
	 */
	public ActorRef addIdRefAssoc(Integer id, ActorRef actor) {
		if (this.idRefMap.containsKey(id))
			return null;
		if (this.idRefMap.containsValue(actor))
			return null;
		return this.idRefMap.put(id, actor);
	}
	
	public ActorRef getActorById(Integer id) {
		return this.idRefMap.get(id);
	}
	
	public Integer getIdByActor(ActorRef actor) {
		Iterator<Integer> keyIter = this.idRefMap.keySet().iterator();
		Integer tmpKey = null;
		while (keyIter.hasNext()) {
			tmpKey = keyIter.next();
			if (this.idRefMap.get(tmpKey).equals(actor))
				return tmpKey;
		}
		return null;
	}
	
	public void removeIdRefEntry(Integer id) {
		this.idRefMap.remove(id);
	}
}
