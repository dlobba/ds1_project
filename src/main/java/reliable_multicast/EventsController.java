package reliable_multicast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import akka.actor.ActorRef;
import reliable_multicast.BaseParticipant.SendMulticastMsg;
import reliable_multicast.messages.FlushMsg;
import reliable_multicast.messages.Message;
import reliable_multicast.messages.crash_messages.*;
import reliable_multicast.messages.crash_messages.MulticastCrashMsg.MutlicastCrashType;
import reliable_multicast.messages.crash_messages.ReceivingCrashMsg.ReceivingCrashType;
import scala.concurrent.duration.Duration;

public abstract class EventsController extends BaseParticipant {
	
	public class SendStepMsg implements Serializable {};
	
	public enum Event {
		MULTICAST_ONE_N_CRASH,
		MULTICAST_N_CRASH,
		RECEIVE_MESSAGE_N_CRASH,
		RECEIVE_VIEW_N_CRASH
	}
	private int step;
	private final Map<String, Event> events;
	private Map<Integer, ActorRef> idRefMap;
	private Map<Integer, Set<Integer>> steps;
	
	public EventsController(boolean manualMode,
							Map<String, Event> events,
							Map<Integer, Set<String>> steps) {
		this(manualMode);
		this.eventsFromMap(events);
		this.stepsFromMap(steps);
		
	}
	
	public EventsController(boolean manualMode) {
		super(manualMode);
		this.step = 0;
		this.events = new HashMap<>();
		this.idRefMap = new HashMap<>();
		this.steps = new HashMap<>();

		// start counting steps
		if (this.manualMode)
			this.scheduleStep();
	}
	
	public EventsController() {
		this(false);
	}
	
	private void eventsFromMap(Map<String, Event> events) {
		if (events == null)
			return;

		String tmpLabel;
		Pattern labelPattern = Pattern.compile("^p([0-9]+)m([0-9]+)$");
		
		Iterator<String> labelIterator =
					events.keySet().iterator();
		while (labelIterator.hasNext()) {
			tmpLabel = labelIterator.next();
			
			if (labelPattern.matcher(tmpLabel).matches()) {
				this.events.put(tmpLabel,
						events.get(tmpLabel));
			}
		}
	}
	
	private void stepsFromMap(Map<Integer, Set<String>> steps) {
		if (steps == null)
			return;
		
		Integer tmpStep;
		Pattern labelPattern = Pattern.compile("^p([0-9]+)$");
		
		Iterator<Integer> stepIterator =
					steps.keySet().iterator();
		while (stepIterator.hasNext()) {
			tmpStep = stepIterator.next();
			this.addStep(tmpStep);
			
			String tmpProcess;
			Iterator<String> processIterator =
					steps.get(tmpStep).iterator();
			while (processIterator.hasNext()) {
				tmpProcess = processIterator.next();
				Matcher processMatch = labelPattern.matcher(tmpProcess);
				if (processMatch.matches()) {
					this.addSender(
							tmpStep,
							Integer.parseInt(processMatch.group(1)));
				}
			}
			
		}
	}
	
	protected void onSendStepMsg(SendStepMsg stepMsg) {
		this.onStep();
		this.scheduleStep();
	}
	
	protected void scheduleStep() {
		int time = new Random().nextInt(MULTICAST_INTERLEAVING);
		this.getContext().getSystem().scheduler()
			.scheduleOnce(Duration.create(time,
					TimeUnit.SECONDS),
					this.getSelf(),
					new SendStepMsg(),
					getContext().system().dispatcher(),
					this.getSelf());
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
	protected void onStep() {
		if (!this.manualMode) {
			return;
		}

		/*
		 * FIRST CHECK:
		 * the step should be stable, so
		 * the two views must be the same.
		 * Only in this way we can be confident
		 * that senders are in the current view
		 * (as expected).
		 */
		if (!this.view.equals(this.tempView)) {
			return;
		}
		/* wait 1 second in order to let other
		 * nodes install the view and **be able** to
		 * send new messages.
		 * In a real system this waiting time should
		 * approximate the worst among network delays.
		 */
		int tmpStep = this.step + 1;
		try {
			Thread.sleep(1000);
			System.out.printf("%d P-%d P-%s INFO step-%d\n",
					System.currentTimeMillis(),
					this.id,
					this.id,
					tmpStep);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		HashSet<Integer> sendersIds = this.getSendersInStep(tmpStep);
		if (sendersIds == null) {
			this.step = tmpStep;
		} else {
			// obtain actorRefs from senders ID
			List<ActorRef> senders = new ArrayList<>();
			for (Integer senderId : sendersIds) {
				senders.add(this.getActorById(senderId));
			}
			
			/* if current view doesn't contain all
			 * the processes required in the step,
			 * then ignore this
			 * step and do it again in the next
			 * scheduling
			 */
			if (this.view.members.containsAll(senders)) {
				// it cannot happen for a sender to be null
				for (ActorRef sender : senders) {
					sender.tell(new SendMulticastMsg(), this.getSelf());
				}
				
				// remove the step, it won't be executed again
				this.steps.remove(this.step);
				// everything went well (hopefully).
				// So, increase the step
				this.step = tmpStep;
			} else {
				System.out.printf("%d P-%d P-%s WARNING missing processes are required for step-%d\n",
						System.currentTimeMillis(),
						this.id,
						this.id,
						tmpStep);
			}
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
		// TODO: add method to msg
		Event event = this.events.get(msg.toString());
		if (event == null)
			return;
		ActorRef sender = this.getActorById(msg.senderID);
		if (sender == null)
			return;
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
		if (!this.steps.keySet().contains(step))
			return null;
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
