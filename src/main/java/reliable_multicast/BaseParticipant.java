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
    public static class SendMulticastMsg implements Serializable {
    };

    // --------------------------------------

    public static final int MULTICAST_INTERLEAVING = 10;
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
    }

    public BaseParticipant(Config config) {
        this(config.isManual_mode());
    }

    public BaseParticipant() {
        this(false);
    }

    // ------------------------------------------

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

    protected void scheduleMessage(Object message, int after) {
        if (message == null)
            return;
        this.getContext()
                .getSystem()
                .scheduler()
                .scheduleOnce(Duration.create(after,
                        TimeUnit.SECONDS),
                        this.getSelf(),
                        message,
                        getContext().system().dispatcher(),
                        this.getSelf());
    }

    /**
     *
     * Send the message simulating a network delay. The delay should be
     * added to a base time, depending on the type of message we want to
     * send. Let's make an assumption now (we should ask to the
     * professor later on if this is fine). Let's suppose that we should
     * implement delays, but delays shouldn't lead to crash failure. In
     * order to so, we could put half of the given base time as value
     * for the delay.
     * 
     * If this is not the case, then suppose that we put a limit T / 2,
     * where T is the base interleaving for a multicast operation. Then
     * we also have that the interleaving for an heartbeat message is T
     * / 2 (as we definded it).
     * 
     * Since heartbeat messages are subject to delays this means that an
     * heartbeat message could take T/2 + T / 2 > T / 2. This would lead
     * to seeing a node as crashed even if it is not. Since the delay
     * trigger the timeout of the heartbeat message.
     * 
     * What do you think? How we should manage delays?
     * 
     * Another important thing: ------------------------ As you may have
     * noticed, I think that it's better to differentiate between
     * control and data message using two separate method and define a
     * common base time for the two. I put the two methods here, but you
     * are free to move them where you want. For example control
     * messages are issue either by group manager and by the participant
     * to send heartbeat messages, so in order to avoid duplicated code
     * this class could be a good fit for them (but you should import
     * the definition of the heartbeat interleaving here for
     * correctness).
     * 
     * Once you have done this, replace occurrences of old sending
     * messages using the new abstraction methods (sendData or
     * sendControl message, respectively). Be careful to think about
     * each scenario where these methods should be used as opposed to
     * the normal method. Potentially every .tell method could be
     * replaced with this, but I think that we should use:
     * 
     * * tell methods: for internal event scheduling and for control
     * messages sent by the EventsController (we should say this during
     * the presentation)
     * 
     * * sendXMessage: for every message (data or
     * control message) that is sent by an actor to another one on a
     * different location.
     * 
     * A final note. To implement delays, remember not to use
     * Thread.sleep for the reason I told you before. Use instead
     * a non blocking approach. Bare in mind that you are
     * on the bello_che_pronto branch, so you should find a method
     * that should fit this already done ;)
     * 
     * PS: once you have completed the work, keep only serious
     * parts of this comment.
     * 
     * @param message
     */
    protected void sendMessage(Object message, int baseTime) {

    }

//    protected void sendControlMessage(Object message) {
//
//    }
//
//    protected void sendDataMessage(Object message) {
//
//    }

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
        System.out
                .printf("%d P-%d P-%d INFO started_view-change V%d\n",
                        System.currentTimeMillis(),
                        this.id,
                        this.id,
                        viewChange.id);
        this.tempView = new View(viewChange.id,
                viewChange.members);
        this.removeOldFlushes(this.tempView.id);

        // TODO: should we send all message up to this view?
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
     * Schedule a new multicast. Be careful that here the behavior will
     * differ between EventsController and normal Participants.
     *
     * In the case of a participant, the scheduling should be ignored if
     * in manual mode.
     *
     * While in the case of the EventController the scheduling
     * represents a step of batch multicasts.
     */
    protected void scheduleMulticast() {
        /*
         * if in manual mode, multicasts are not sent automatically, so
         * block the scheduling.
         */
        if (this.manualMode)
            return;
        int time = new Random().nextInt(MULTICAST_INTERLEAVING);
        this.scheduleMessage(new SendMulticastMsg(), time);
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
        /*
         * if the sender is not in the view, then do not accept the
         * message
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
            // TODO: should we deliver this message
            // in the current view?
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
