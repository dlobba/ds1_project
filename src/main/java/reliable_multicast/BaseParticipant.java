package reliable_multicast;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
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

    public static final int MAX_DELAY_TIME = 4;
    public static final int MULTICAST_INTERLEAVING =
            MAX_DELAY_TIME / 2;
    public static final int MAX_TIMEOUT =
            MAX_DELAY_TIME * 2 + 1;

    
    // not in using as of now...
    //public static final int MAX_DELAY = MULTICAST_INTERLEAVING / 2;

    protected int id;
    protected int multicastId;
    // the set of actors that are seen by this node
    protected View view;
    // temporary view established in the all-to-all
    // phase
    protected View tempView;
    protected boolean canSend;
    protected Set<FlushMsg> flushesReceived;
    protected Set<Message> messagesUnstable;

    // store the status of all participants
    // from the point of view of this actor
    protected final Map<Integer, Integer> processLastCall;

    // this was used to debug alives messages
    protected int aliveId;
    protected boolean manualMode;

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

    // ------- CONSTRUCTORS ---------------------

    public BaseParticipant(boolean manualMode) {
        super();
        this.resetParticipant();
        this.manualMode = manualMode;
        this.processLastCall = new HashMap<>();
    }

    public BaseParticipant(Config config) {
        this(config.isManual_mode());
    }

    public BaseParticipant() {
        this(false);
    }

    // ------------------------------------------

    private void updateProcessLastCall(Integer processId,
            Integer messageID) {
        if (processId == -1)
            return;
        this.processLastCall.put(processId, messageID);
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

    /**
     * Send a message from a node to another node emulating
     * network delays. The delay is set to be between
     * T/2 and T, without exceeding T.
     * 
     * @param message
     * @param baseTime
     * @param receiver
     */
    protected void sendNetworkMessage(Object message, ActorRef receiver) {
        int time = new Random().nextInt(MAX_DELAY_TIME / 2)
                + MAX_DELAY_TIME / 2;
        scheduleMessage(message, time, receiver);
    }

    /**
     * Send a network message after a fixed amount of time.
     *
     * @param message
     * @param after
     * @param receiver
     */
    protected void sendNetworkMessageAfter(Object message, int after, ActorRef receiver) {
        int time = new Random().nextInt(MAX_DELAY_TIME / 2)
                + MAX_DELAY_TIME / 2;
        scheduleMessage(message, after + time, receiver);
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

        System.out.printf("%d P-%d P-%d INFO started_view-change V%d\n",
                System.currentTimeMillis(),
                this.id,
                this.id,
                viewChange.id);
        this.tempView = new View(viewChange.id,
                viewChange.members,
                viewChange.membersIds);
        this.removeOldFlushes(this.tempView.id);

        // TODO: should we send all message up to this view?
        for (Message message : messagesUnstable) {
            for (ActorRef member : this.tempView.members) {
                sendNetworkMessage(message, member);
            }
        }
        // FLUSH messages: send them after having sent
        // all ViewChange messages. This is guaranteed
        // by sending after MAX_DELAY_TIME
        for (ActorRef member : this.tempView.members) {
            sendNetworkMessageAfter(new FlushMsg(this.id,
                    this.tempView.id,
                    this.getSelf()),
                    MAX_DELAY_TIME + 1,
                    member);
        }
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
            // TODO: deliver all mesages up to current view
            this.deliverAllMessages();
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
     */
    protected void scheduleMulticast() {
        /*
         * if in manual mode, multicasts are not sent automatically, so
         * block the scheduling.
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
                this.tempView.id,
                false);
        this.multicastId += 1;
        System.out.printf("%d send multicast %d within %d",
                this.id,
                this.multicastId,
                this.view.id);
        for (ActorRef member : this.view.members) {
            sendNetworkMessage(message, member);
        }
        // STABLE messages
        message = new Message(message, true);
        for (ActorRef member : this.view.members) {
            sendNetworkMessageAfter(message,
                    MAX_DELAY_TIME + 1,
                    member);
        }
        this.canSend = true;
    }

    protected void onReceiveMessage(Message message) {
        /*
         * if the sender is not in the view, then do not accept the
         * message
         */
        if (!this.tempView.members.contains(this.getSender()))
            return;
        // TODO: rewrite completely this part. It's wrong.
        System.out.printf("%d deliver multicast %d from %d within %d\n",
                System.currentTimeMillis(),
                this.id,
                message.messageID,
                message.senderID,
                message.viewId);
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
            // TODO: should we deliver this message
            // in the current view?
            this.deliverMessage(message);
        }
        // update the status of the system within the pov
        // of this actor
        this.updateProcessLastCall(message.senderID,
                message.messageID);
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
