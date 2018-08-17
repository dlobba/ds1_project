package reliable_multicast;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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

public abstract class BaseParticipant extends AbstractActor {

    // --- Messages for internal behavior ---
    public static class SendMulticastMsg implements Serializable {};

    // --------------------------------------

    public static final int MAX_DELAY_TIME = 4;
    public static final int MULTICAST_INTERLEAVING =
            MAX_DELAY_TIME / 2;
    public static final int MAX_TIMEOUT =
            MAX_DELAY_TIME * 2 + 1;

    protected int id;
    protected int multicastId;
    // the set of actors that are seen by this node
    protected View view;
    // temporary view established in the all-to-all
    // phase. The temporary view is equal to the current view
    // only when the view is effectively installed.
    protected View tempView;
    protected boolean canSend;
    protected Set<FlushMsg> flushesReceived;
    protected Set<Message> messagesBuffer;

    // store the status of all participants
    // from the point of view of this actor
    protected final Map<Integer, Integer> processesDelivered;

    protected boolean manualMode;

    protected void resetParticipant() {
        this.id = -1;
        this.multicastId = 0;
        this.canSend = false;
        this.view = new View(-1);
        this.tempView = new View(-1);
        this.messagesBuffer = new HashSet<>();
        this.flushesReceived = new HashSet<>();
    }

    // ------- CONSTRUCTORS ---------------------

    public BaseParticipant(boolean manualMode) {
        super();
        this.resetParticipant();
        this.manualMode = manualMode;
        this.processesDelivered = new HashMap<>();
    }

    public BaseParticipant(Config config) {
        this(config.isManual_mode());
    }

    public BaseParticipant() {
        this(false);
    }

    // ------------------------------------------

    private void updateProcessesDelivered(Integer processId,
            Integer messageID) {
        if (processId == -1)
            return;
        this.processesDelivered.put(processId, messageID);
    }

    protected void removeOldFlushes(int currentView) {
        Iterator<FlushMsg> msgIterator =
                this.flushesReceived.iterator();
        FlushMsg flushMsg;
        while (msgIterator.hasNext()) {
            flushMsg = msgIterator.next();
            if (flushMsg.viewID < currentView)
                msgIterator.remove();
        }
    }

    protected void scheduleMessage(Object message, int after, ActorRef receiver) {
        if (message == null)
            return;
        this.getContext()
        .getSystem()
        .scheduler()
        .scheduleOnce(Duration.create(after,
                TimeUnit.SECONDS),
                receiver,
                message,
                getContext().system().dispatcher(),
                this.getSelf());
    }

    protected void sendInternalMessage(Object message, int time) {
        scheduleMessage(message, time, this.getSelf());
    }

    /**
     * An abstraction used for internal messages
     * used to schedule lookup procedures, like
     * alive checking.
     * @param message
     */
    protected void sendTimeoutMessage(Object message) {
        sendInternalMessage(message,
                MAX_TIMEOUT);
    }

    protected void sendTimeoutMessageAfter(Object message, int after) {
        sendInternalMessage(message,
                after + MAX_TIMEOUT);
    }

    /**
     * Send a message after a fixed amount of time.
     *
     * @param message
     * @param after
     * @param receiver
     */
    protected void sendMessageAfter(Object message, int after, ActorRef receiver) {
        scheduleMessage(message, after, receiver);
    }

    /**
     * Send a multicast simulating delays.
     * Each message is sent with 1 second interleaving
     * between each other.
     * 
     * @param message
     * @param baseTime time after messages start to be sent
     * @return the estimated time after all messages will be
     * sent.
     */
    protected int delayedMulticast(Object message,
            Set<ActorRef> receivers,
            int baseTime) {
        int time = 0;
        for (ActorRef receiver : receivers) {
            sendMessageAfter(message, baseTime + time, receiver);
            time += 1;
        }
        return time;
    }

    protected int delayedMulticast(Object message,
            Set<ActorRef> receivers) {
        return this.delayedMulticast(message, receivers, 0);
    }

    /**
     * Return the set of actors associated to the
     * flush messages received.
     * 
     * @param currentView
     * @return
     */
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
    
    protected Message getMessage(Message other) {
        Iterator<Message> msgIter = this.messagesBuffer.iterator();
        Message tmp;
        while (msgIter.hasNext()) {
            tmp = msgIter.next();
            if (tmp.equals(other))
                return tmp;
        }
        return null;
    }

    protected void onStopMulticast(StopMulticastMsg stopMsg) {
        this.canSend = false;
        System.out
        .printf("%d P-%d P-%d INFO stopped_multicasting\n",
                System.currentTimeMillis(),
                this.id,
                this.id);
    }

    protected void onViewChangeMsg(ViewChangeMsg viewChange) {
        // do not install an old view
        if (viewChange.id < this.tempView.id)
            return;

        System.out.printf("%d P-%d P-%d INFO started_view_change V%d\n",
                System.currentTimeMillis(),
                this.id,
                this.id,
                viewChange.id);
        this.tempView = new View(viewChange.id,
                viewChange.members,
                viewChange.membersIds);
        this.removeOldFlushes(this.tempView.id);

        int waitTime = 0;
        for (Message message : messagesBuffer) {
            // mark the message as stable
            message = new Message(message, true);
            waitTime = this.delayedMulticast(message, this.tempView.members);
        }
        // FLUSH messages: send them after having sent
        // all ViewChange messages. This is guaranteed
        // by sending after waitTime (+1 to be sure)
        this.delayedMulticast(new FlushMsg(this.id,
                    this.tempView.id,
                    this.getSelf()),
                this.tempView.members,
                waitTime + 1);
    }

    protected void onFlushMsg(FlushMsg flushMsg) {
        /*
         * if the flush is for a previous view-change then just ignore
         * it.
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
            this.view = new View(tempView);
            System.out.printf("%d install view %d %s\n",
                    this.id,
                    this.view.id,
                    this.view.logMembers());
            System.out.printf("%d P-%d P-%d installed_view %s\n",
                    System.currentTimeMillis(),
                    this.id,
                    this.id,
                    this.view.toString());
            System.out.printf("%d P-%d P-%d INFO can_send\n",
                    System.currentTimeMillis(),
                    this.id,
                    this.id);
            // deliver all mesages up to current view
            Iterator<Message> msgIter = messagesBuffer.iterator();
            Message message;
            while (msgIter.hasNext()) {
                message = msgIter.next();
                if (message.viewId <= this.view.id) {
                    deliverMessage(message);
                    msgIter.remove();
                }
            }
            // resume multicasting
            this.canSend = true;
            this.scheduleMulticast();
        }
    }

    protected void onSendMulticastMsg(SendMulticastMsg message) {
        this.scheduleMulticast();
        this.multicast();
    }

    /**
     * Schedule a new multicast.
     */
    protected void scheduleMulticast() {
        /*
         * if in manual mode, multicasts are not sent automatically,
         * so block the scheduling.
         */
        if (this.manualMode)
            return;
        sendInternalMessage(new SendMulticastMsg(),
                MULTICAST_INTERLEAVING);
    }

    private void multicast() {
        if (!this.canSend)
            return;
        // this node cannot send message
        // until this one is completed
        this.canSend = false;
        Message message = new Message(
                this.id,
                this.multicastId,
                this.view.id,
                false);
        System.out.printf("%d send multicast %d within %d\n",
                this.id,
                this.multicastId,
                this.view.id);
        System.out.printf("%d P-%d P-%d multicast_message %s\n",
                System.currentTimeMillis(),
                this.id,
                this.id,
                message.toString());
        this.multicastId += 1;
        int waitTime;
        waitTime = this.delayedMulticast(message, this.view.members);
        // STABLE messages
        message = new Message(message, true);
        this.delayedMulticast(message, this.view.members, waitTime);
        this.canSend = true;
    }

    protected void onReceiveMessage(Message message) {
        /*
         * if the sender is not in the view, then do not accept the
         * message
         */
        if (!this.tempView.members.contains(this.getSender()))
            return;
        // a message is crossing the view boundary.
        // ignore it
        if (message.viewId > this.view.id) {
            return;
        }
        if (message.viewId < this.view.id)
            return;
        /* Check if the id of the new message is greater
         * than the last delivered message coming from
         * the sender process. If this is the case
         * then deliver the message.
         */
        deliverMessage(message);
        if (message.stable)
            this.messagesBuffer.remove(message);
        else
            this.messagesBuffer.add(message);
    }

    /**
     * Deliver the message given if there is not
     * a more recent message delivered from the
     * sender process with regards to the point of
     * view of the current process (as in vector clocks).
     * @param message
     */
    protected void deliverMessage(Message message) {
        if (this.processesDelivered
                .get(message.senderID) == null ||
                this.processesDelivered
                .get(message.senderID) < message.messageID) {
            System.out.printf("%d deliver multicast %d from %d within %d\n",
                    this.id,
                    message.messageID,
                    message.senderID,
                    message.viewId);
            System.out.printf("%d P-%d P-%d delivered_message %s\n",
                  System.currentTimeMillis(),
                  this.id,
                  message.senderID,
                  message.toString());
            // update the mapping
            this.updateProcessesDelivered(message.senderID,
                    message.messageID);
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
