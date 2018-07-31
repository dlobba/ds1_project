package reliable_multicast;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.util.Timeout;
import reliable_multicast.messages.*;
import reliable_multicast.messages.events_messages.MulticastCrashMsg;
import reliable_multicast.messages.events_messages.ReceivingCrashMsg;
import reliable_multicast.messages.step_message.StepMessage;
import scala.concurrent.Await;

public class Participant extends BaseParticipant {

    public static class CheckGmAliveMsg implements Serializable {
    };

    protected ActorRef groupManager;
    protected boolean crashed;

    protected boolean receiveMessageAndCrash;
    protected boolean receiveViewChangeAndCrash;
    private String ignoreMessageLabel;

    private boolean isGmAlive;

    /*
     * This will be called in the constructor by issuing the super()
     * method.
     * 
     * @see reliable_multicast.BaseParticipant#resetParticipant()
     */
    @Override
    protected void resetParticipant() {
        super.resetParticipant();
        this.receiveMessageAndCrash = false;
        this.receiveViewChangeAndCrash = false;
    }

    // Constructors -----------------------------

    public Participant(ActorRef groupManager, boolean manualMode) {
        super(manualMode);
        this.groupManager = groupManager;
        this.crashed = false;
        this.isGmAlive = true;
        sendNetworkMessage(new JoinRequestMsg(), groupManager);
    }

    public Participant(ActorRef groupManager) {
        this(groupManager, false);
    }

    public Participant(String groupManagerPath, boolean manualMode) {
        super(manualMode);
        this.crashed = false;
        this.isGmAlive = true;
        this.groupManager = null;
        
//        Timeout timeout = new Timeout(MAX_DELAY_TIME, java.util.concurrent.TimeUnit.SECONDS); 
//        ActorRef groupManagerRef;
//        try {
//            groupManagerRef = Await.result(getContext()
//                    .actorSelection(groupManagerPath)
//                    .resolveOne(timeout), 
//                    timeout.duration());
//
//            sendNetworkMessage(new JoinRequestMsg(),
//                    groupManagerRef);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        getContext().actorSelection(groupManagerPath)
        .tell(new JoinRequestMsg(),
                this.getSelf());
    }

    public Participant(String groupManagerPath) {
        this(groupManagerPath, false);
    }

    public static Props props(ActorRef groupManager,
            boolean manualMode) {
        return Props.create(Participant.class,
                () -> new Participant(groupManager, manualMode));
    }

    public static Props props(ActorRef groupManager) {
        return props(groupManager, false);
    }

    public static Props props(String groupManagerPath,
            boolean manualMode) {
        return Props.create(Participant.class,
                () -> new Participant(groupManagerPath, manualMode));
    }

    public static Props props(String groupManagerPath) {
        return props(groupManagerPath, false);
    }

    // --------------------------------

    /*
     * When a participant receives a JoinRequestMsg it's only when the
     * Group Manager answers to the request with the ID for the
     * participant.
     */
    private void onJoinMsg(JoinRequestMsg joinResponse) {
        if (this.crashed)
            return;
        this.id = joinResponse.idAssigned;
        this.groupManager = this.getSender();
        //this.isGmAlive = true;
        System.out
        .printf("%d P-%d P-%s JOIN-ASSOC\n",
                System.currentTimeMillis(),
                this.id,
                this.getSelf().path().name());
        // start checking the groupmanager
        sendTimeoutMessage(new CheckGmAliveMsg());
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
        } else
            super.onViewChangeMsg(viewChange);
    }

    /**
     * Replicate the behavior of the normal view-change method, but
     * instead of sending the FLUSH messages the node crashes.
     * 
     * @param viewChange
     */
    protected void crashAfterViewChange(ViewChangeMsg viewChange) {
        System.out
                .printf("%d P-%d P-%d INFO started_view_change V%d\n",
                        System.currentTimeMillis(),
                        this.id,
                        this.id,
                        viewChange.id);
        this.flushesReceived.clear();
        this.tempView = new View(viewChange.id,
                viewChange.members);
        for (Message message : messagesUnstable) {
            for (ActorRef member : this.tempView.members) {
                sendNetworkMessage(message, member);
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
     * This message was used before having the event-handler system
     * based on config.
     * 
     * Now the message is kept to let the node crash from the outside
     * and without using a config file (we won't use this approach).
     * 
     * We make a node crash after a particular event, like multicast and
     * crash, or receive a message and crash.
     * 
     * @param crashMsg
     */
    private void onCrashMsg(CrashMsg crashMsg) {
        this.crash();
    }

    private void onAliveMsg(AliveMsg aliveMsg) {
        if (this.crashed)
            return;
        sendNetworkMessage(new AliveMsg(this.aliveId, this.id), 
                this.getSender());
    }

    /**
     * Turn off crashed mode and ask the group manager to join.
     * 
     * @param reviveMsg
     */
    private void onReviveMsg(ReviveMsg reviveMsg) {
        this.crashed = false;
        // TODO: remove the node from the crashed node
        sendNetworkMessage(new JoinRequestMsg(),
                this.groupManager);
    }

    // implementing sending and receiving -------
    // variants with crashes --------------------

    private void multicastAndCrash() {
        if (!this.canSend)
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
            sendNetworkMessage(message, member);
        }
        // Do not send stable messages.
        // Crash instead
        this.crash();
    }

    /**
     * While performing a multicast, the message is effectively sent
     * just to one other actor, then the sender crashes.
     * 
     * This method tries to show the particular case in which an actor
     * is able to send a message to one actor before crashing (this is
     * what this actor will do) and the receiving actor crashes after
     * seeing the message.
     * 
     * So no operational particiapant will see the message, although
     * some crashed node received (and could have delivered) the
     * message.
     * 
     * For this to happen we want to send the message to any other node
     * but the group manager (which cannot crash). So we require at
     * least 3 actors (which means the group manager and two
     * participants) to be in the view.
     * 
     * Note: The crash of the receiving actor is not defined here. It's
     * possible to obtain the scenario with a particularly crafted
     * configuration file (test1.json tries to describe exactly this
     * scenario).
     */
    private void multicastOneAndCrash() {
        if (!this.canSend)
            return;
        // this node cannot send messages
        // until this one is completed
        this.canSend = false;

        Set<ActorRef> members = this.view.members;
        if (members.size() < 3) {
            System.out.printf("%d P-%d P-%s WARNING: too few view" +
                    " members. Two participants and the" +
                    " group manager are required." +
                    " Crash denied. Multicast aborted. \n",
                    System.currentTimeMillis(),
                    this.id,
                    this.id);
            return;
        }

        /*
         * Avoid choosing the group manager or self as receiver.
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
        Message message = new Message(this.id,
                this.multicastId,
                this.tempView.id,
                false);
        this.multicastId += 1;
        sendNetworkMessage(message, receiver);
        // let the sender crash
        this.crash();
    }

    protected void onSendMutlicastCrashMsg(MulticastCrashMsg crashMsg) {
        switch (crashMsg.type) {
        case MULTICAST_N_CRASH:
            System.out.printf(
                    "%d P-%d P-%s INFO process will multicast then crash\n",
                    System.currentTimeMillis(),
                    this.id,
                    this.id);
            this.multicastAndCrash();
            break;
        case MULTICAST_ONE_N_CRASH:
            System.out.printf("%d P-%d P-%s INFO process will multicast to one" +
                    " participant then crash\n",
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
            System.out.printf("%d P-%d P-%s INFO process set to crash on" +
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
        if (crashed)
            return;
        
         //DEBUG: 
         System.out.printf("%d P-%d P-%d INFO Checking Group Manager\n",
                 System.currentTimeMillis(),
                 this.id,
                 this.id);
        if (!isGmAlive) {
            System.out.printf("%d P-%d P-%d INFO Group manager Unreachable." +
                    " Exiting...\n",
                    System.currentTimeMillis(),
                    this.id,
                    this.id);
            this.getContext().stop(this.getSelf());
            this.getContext().system().terminate();
        } else {
            isGmAlive = false;
            sendNetworkMessage(new GmAliveMsg(), groupManager);
            sendTimeoutMessage(new CheckGmAliveMsg());
        }
    }

    private void onGmAliveMsg(GmAliveMsg msg) {
    	 isGmAlive = true;
    	if (crashed)
            return;
         // DEBUG: 
         System.out.printf("%d P-%d P-%d received_gm_alive_message\n",
                 System.currentTimeMillis(),
                 this.id,
                 this.id);
         
    }
    
    // DEBUG:
    private void onStepMessage(StepMessage msg) {
    	System.out.printf("%d P-%d P-%s INFO step-%d\n",
                System.currentTimeMillis(),
                this.id,
                this.id,
                msg.id);
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
                //.match(CheckGmAliveMsg.class, this::onCheckGmAliveMsg)
                .match(GmAliveMsg.class, this::onGmAliveMsg)
                //DEBUG:
                .match(StepMessage.class,  this::onStepMessage)
                .build();
    }
}
