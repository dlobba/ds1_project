package reliable_multicast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import reliable_multicast.messages.Message;
import reliable_multicast.messages.ReviveMsg;
import reliable_multicast.messages.crash_messages.*;
import reliable_multicast.messages.crash_messages.MulticastCrashMsg.MutlicastCrashType;
import reliable_multicast.messages.crash_messages.ReceivingCrashMsg.ReceivingCrashType;
import reliable_multicast.utils.EventsList;
import reliable_multicast.utils.IdRefMap;
import reliable_multicast.utils.StepProcessMap;
import scala.concurrent.duration.Duration;

public abstract class EventsController extends BaseParticipant {
	
	public class SendStepMsg implements Serializable {};
	
	public enum Event {
		MULTICAST_ONE_N_CRASH,
		MULTICAST_N_CRASH,
		RECEIVE_MESSAGE_N_CRASH,
		RECEIVE_VIEW_N_CRASH
	}
	protected IdRefMap aliveProcesses;
	protected IdRefMap crashedProcesses;
	private int step;
	private StepProcessMap sendOrder;
	private StepProcessMap risenOrder;
	private StepProcessMap views;
	private EventsList events;
	
	public EventsController(boolean manualMode,
							Map<String, Event> events,
							Map<Integer, Set<String>> sendOrder,
							Map<Integer, Set<String>> risenOrder,
							Map<Integer, Set<String>> views) {
		this(manualMode);
		this.events.fromMap(events);
		this.sendOrder.fromMap(sendOrder);
		this.risenOrder.fromMap(risenOrder);
		this.views.fromMap(views);
	}
	
	public EventsController(boolean manualMode) {
		super(manualMode);
		this.step = 0;
		this.events = new EventsList();
		this.aliveProcesses = new IdRefMap();
		this.crashedProcesses = new IdRefMap();
		this.sendOrder = new StepProcessMap();
		this.risenOrder = new StepProcessMap();
		this.views = new StepProcessMap();

		// start counting steps
		if (this.manualMode)
			this.scheduleStep();
	}
	
	public EventsController() {
		this(false);
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
	 * Intercept the message and update the last call
	 * value associated to the sender.
	 */
	@Override
	protected void onReceiveMessage(Message message) {
		super.onReceiveMessage(message);
		this.events.updateProcessLastCall(message.senderID, message.messageID);
	}
	
	/**
	 * This defines a step by the event
	 * controller.
	 * 
	 * If in manual mode, the set of senders
	 * associated in this step is picked and
	 * the controller sends a message to these
	 * senders that allow them to call a multicast.
	 * 
	 * In the same way, crashed process can be revived.
	 * 
	 * Correctness is enforced (it should at least)
	 * defining the required process in the view set,
	 * which contains either processes that must be
	 * alive or crashed in order for the step to work
	 * as expected.
	 */
	protected void onStep() {
		if (!this.manualMode) {
			return;
		}
		int tmpStep = this.step + 1;
		System.out.printf("%d P-%d P-%s INFO step-%d\n",
				System.currentTimeMillis(),
				this.id,
				this.id,
				tmpStep);
		/*
		 * FIRST CHECK:
		 * the step should be stable, so
		 * the two views must be the same.
		 * Only in this way we can be confident
		 * that senders are in the current view
		 * (as expected).
		 */
		if (!this.view.equals(this.tempView)) {
			System.out.printf("%d P-%d P-%s INFO View unstable\n",
					System.currentTimeMillis(),
					this.id,
					this.id);
			return;
		}
		
		/* SECOND CHECK:
		 * the view (defined within the view set
		 * in the config) is defined and all processes
		 * within the view are effectively the participants
		 * in the current view seen by the controller
		 */
		Set<Integer> viewParticipantsIds =
				this.views.getProcessesInStep(tmpStep);
		if (viewParticipantsIds == null) {
			this.step = tmpStep;
			return;
		}
		/* retrieve actorRefs associated to their ids
		 * If a process is not associated to an ActorRef
		 * then ignore the step (the required process
		 * has to join the view yet).
		 */
		List<ActorRef> participants = new ArrayList<>();
		for (Integer processId : viewParticipantsIds) {
			if (this.aliveProcesses.getActorById(processId) != null) {
				participants.add(this.aliveProcesses
									 .getActorById(processId));
			} else if (this.crashedProcesses
						   .getActorById(processId) != null) {
				participants.add(this.crashedProcesses
						 			 .getActorById(processId));
			} else {
				System.out.printf("%d P-%d P-%s INFO missing processes " + 
						"in the view for step-%d\n",
						System.currentTimeMillis(),
						this.id,
						this.id,
						tmpStep);
				return;
			}
		}
		/* if all participants required in the step
		 * are in the current view then actions within
		 * the step can be executed.
		 * Otherwise ignore the step... some desired process
		 * is missing...
		 */
		Set<ActorRef> currentParticipants = new HashSet<>(this.tempView.members);
		currentParticipants.addAll(this.crashedProcesses.getProcessesActors());
		if (!currentParticipants.containsAll(participants)) {
			return;
		}
		
		/*
		 * here we are sure that processes defined
		 * in the senders section, in the views section or
		 * in the risen section are either alive or have been
		 * crashed (but still are in the system, so no errors).
		 */
		
		/* 
		 * Here we try to manage a small issue.
		 * We can have only the point of view of the
		 * controller, so when a view change occurs the
		 * controller could have correctly installed the
		 * view, while other participants have not (it's
		 * likely to be just a matter of milliseconds). If they
		 * have not installed the view yet, then they
		 * are not able to send multicasts, so orders
		 * stated by the controller won't be performed.
		 * 
		 * So, let's wait 1 second in order to let other
		 * nodes install the view and **be able** to
		 * send new messages.
		 * In a real system this waiting time should
		 * approximate the worst among network delays.
		 */
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// collect the senders set and the set
		// of processes to revive
		Set<Integer> sendersIds =
				this.sendOrder.getProcessesInStep(tmpStep);
		Set<Integer> risenIds =
				this.risenOrder.getProcessesInStep(tmpStep);
		
		boolean missingSenders = sendersIds != null &&
								 !viewParticipantsIds
								 	.containsAll(sendersIds);
		boolean missingRisen = risenIds != null &&
							   !this.crashedProcesses
							   		.getProcessesIds()
							   		.containsAll(risenIds);
		if (missingRisen || missingSenders) {
			System.out.printf("%d P-%d P-%s WARNING wrong" +
							  " processes configuration for step-%d\n",
							  System.currentTimeMillis(),
							  this.id,
							  this.id,
							  tmpStep);
			return;
		}
		
		/*
		 * While picking senders, check whether
		 * an event can be triggered.
		 * If this is the case, store the id for
		 * further processing.
		 * If a process has associated both
		 * a normal multicast and a crashing
		 * multicast, then avoid doing the
		 * first multicast. In the case
		 * of a receiving event (like receive
		 * and crash) then allow both.
		 */
		ActorRef tmpSender;
		List<ActorRef> senders = new ArrayList<>();
		String nextEventLabel;
		List<Integer> triggeringIds = new ArrayList<>();
		if (sendersIds != null) {
			// obtain actorRefs from senders ID
			for (Integer senderId : sendersIds) {
				tmpSender = this.aliveProcesses
							 .getActorById(senderId);
				// it can happen for a sender to be null.
				// For instance when the process is crashed yet the
				// configuration file states the process to send
				// a multicast. The configuration file is therefore
				// wrong.
				if (tmpSender == null) {
					System.out.printf("%d P-%d P-%s ERROR process p%d" + 
							" cannot send in step-%d. It's crashed. Check the conf. file.\n",
							System.currentTimeMillis(),
							this.id,
							this.id,
							senderId,
							tmpStep);
					return;
				}
				nextEventLabel = this.events.getProcessNextLabel(senderId);
				// if the process has an event associated, then
				// the retrieved string cannot be null.
				// if it's null then just add the sender
				if (nextEventLabel == null)
					senders.add(tmpSender);
				else {
					triggeringIds.add(senderId);
					if (!this.events.isSendingEvent(nextEventLabel))
						senders.add(tmpSender);
					else {
						System.out.printf("%d P-%d P-%s WARNING process p%d" + 
								" had two concurrent sending events in step-%d." +
								" The normal multicast has been ignored." +
								" Check the conf. file.\n",
								System.currentTimeMillis(),
								this.id,
								this.id,
								senderId,
								tmpStep);
					}
				}
			}
		}
		System.out.printf(triggeringIds.toString());
		
		ActorRef crashedProcess;
		List<ActorRef> risenList = new ArrayList<>();
		if (risenIds != null) {
			// obtain actorRefs from risen processes ID
			for (Integer risenId : risenIds) {
				crashedProcess = this.crashedProcesses
						.getActorById(risenId);
				if (crashedProcess == null) {
					System.out.printf("%d P-%d P-%s ERROR process p%d" + 
							" cannot revive in step-%d. It's still alive. Check the conf. file.\n",
							System.currentTimeMillis(),
							this.id,
							this.id,
							risenId,
							tmpStep);
					return;
				}
				risenList.add(crashedProcess);
			}
		}
		
		/*
		 * DONE
		 * all checks are done. It's safe to blindly
		 * send messages.
		 */
		
		for (ActorRef sender : senders) {
			sender.tell(new SendMulticastMsg(), this.getSelf());
		}
		for (Integer triggeringId : triggeringIds) {
			this.triggerEvent(triggeringId);
		}		
		for (ActorRef risen : risenList) {
			risen.tell(new ReviveMsg(), this.getSelf());
			this.crashedProcesses
				.removeIdRefEntry(this.crashedProcesses.getIdByActor(risen));
		}
		
		// everything went well (hopefully).
		// So, increase the step
		this.step = tmpStep;
	}
	
	/**
	 * Check whether the sender Id
	 * has an event associated. If this is the case
	 * then generate the associated message and send it
	 * to the correct receiver (as of now it's the actor
	 * associated with the sender ID).
	 * 
	 */
	protected void triggerEvent(Integer processId) {
		String eventLabel = this.events.getProcessNextLabel(processId);
		Event event = this.events.getEvent(eventLabel);
		if (event == null)
			return;
		ActorRef sender = this.aliveProcesses
							  .getActorById(processId);
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
}
