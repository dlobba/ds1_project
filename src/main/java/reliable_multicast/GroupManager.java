package reliable_multicast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import akka.actor.ActorRef;
import akka.actor.Props;
import reliable_multicast.messages.AliveMsg;
import reliable_multicast.messages.FlushMsg;
import reliable_multicast.messages.GmAliveMsg;
import reliable_multicast.messages.JoinRequestMsg;
import reliable_multicast.messages.Message;
import reliable_multicast.messages.StopMulticastMsg;
import reliable_multicast.messages.ViewChangeMsg;

public class GroupManager extends EventsController {

    public static class CheckViewMsg implements Serializable {
    };

    // id generator used for ID assignment to
    // nodes joining the system
    private int idPool;

    /*
     * alivesReceives will contain all actors in the system. On a
     * regular basis each actor will be asked to answer to an heartbeat
     * message (AliveMsg). If it does, the actor is removed from the
     * set.
     *
     * alivesReceived will therefore contain actors that haven't sent
     * back an answer to the hearbeat within a given time slot, so they
     * are seen as crashed nodes.
     */
    private Set<ActorRef> alivesReceived;

    private void initGroupManager(int id) {
        this.id = id;
        this.aliveProcesses
                .addIdRefAssoc(this.id, this.getSelf());
        this.idPool = this.id + 1;
        this.alivesReceived = new HashSet<>();

        // The Group Manager is the first
        // element of the view
        HashSet<ActorRef> initialView = new HashSet<ActorRef>();
        initialView.add(this.getSelf());
        this.view = new View(0, initialView);
        this.tempView = new View(view);
        System.out.printf("%d P-%d P-%d INFO Group_manager_initiated\n",
                System.currentTimeMillis(),
                this.id,
                this.id);
        System.out.printf("%d P-%d P-%d INFO View %s\n",
                System.currentTimeMillis(),
                this.id,
                this.id,
                this.view.toString());
        this.canSend = true;
        // start checking the view
        //sendTimeoutMessage(new CheckViewMsg());
        this.getSelf().tell(new CheckViewMsg(), this.getSelf());
    }

    // --------- CONSTRUCTORS ------------------

    /**
     * Constructor thought to be used when manual mode is wanted.
     *
     * @param id
     * @param manualMode
     * @param events
     * @param steps
     */
    public GroupManager(
            int id,
            boolean manualMode,
            Map<String, Map<Event, Set<String>>> events,
            Map<Integer, Set<String>> sendOrder,
            Map<Integer, Set<String>> risenOrder,
            Map<Integer, Set<String>> views) {
        super(manualMode, events, sendOrder, risenOrder, views);
        this.initGroupManager(id);

    }

    /**
     * Constructor thought to be used when auto mode is wanted.
     *
     * @param id
     */
    public GroupManager(int id) {
        super(false);
        this.initGroupManager(id);
    }

    public static Props props(int id,
            boolean manualMode,
            Map<String, Map<Event, Set<String>>> events,
            Map<Integer, Set<String>> sendOrder,
            Map<Integer, Set<String>> risenOrder,
            Map<Integer, Set<String>> views) {
        return Props.create(GroupManager.class,
                () -> new GroupManager(id, manualMode, events,
                        sendOrder, risenOrder,
                        views));
    }

    public static Props props(int id) {
        return Props.create(GroupManager.class,
                () -> new GroupManager(id));
    }

    // ------------------------------------------

    public int getIdPool() {
        return idPool;
    }

    public Set<ActorRef> getAlivesReceived() {
        return new HashSet<>(this.alivesReceived);
    }

    private void onJoinRequestMsg(JoinRequestMsg request) {
        // DEBUG:
        System.out.printf("%d P-%s P-%s INFO join_request\n",
                System.currentTimeMillis(),
                this.getSelf().path().name(),
                this.getSender().path().name());

        JoinRequestMsg response = new JoinRequestMsg(this.idPool);
        sendNetworkMessage(response, this.getSender());

        // add a new entry to the association map
        this.aliveProcesses
                .addIdRefAssoc(this.idPool, this.getSender());
        this.idPool += 1;

        // define the new view
        // we start from the last temporary view since it's
        // the most up to date.
        Set<ActorRef> newView = new HashSet<>(this.tempView.members);
        newView.add(this.getSender());
        onViewChange(newView);
    }

    @Override
    protected void onReceiveMessage(Message message) {
        super.onReceiveMessage(message);
    }

    private void onViewChange(Set<ActorRef> newMembers) {
        // tell every member in the view to stop
        // generating new multicasts
        for (ActorRef member : newMembers) {
            sendNetworkMessage(new StopMulticastMsg(), member);
        }

        // Due to FIFO guarantees given by the Akka
        // framework, we are (we should) be safe not
        // send the view change before acknowledging
        // everyone has stopped sending multicasts.
        this.tempView = new View(this.tempView.id + 1,
                newMembers);
        System.out.printf("%d P-%d P-%d INFO change-view: %s\n",
                System.currentTimeMillis(),
                this.id,
                this.id,
                this.tempView.toString());
        ViewChangeMsg viewMsg = new ViewChangeMsg(this.tempView);
        for (ActorRef member : newMembers) {
            sendNetworkMessage(viewMsg, member);
        }
    }

    /*
     * Send a message to each member in the view. If a response has not
     * been received by some member (from a previous call to the method)
     * then issue a view change.
     */
    private void onCheckViewMsg(CheckViewMsg msg) {
        
         // DEBUG:
         System.out.printf("%d P-%d P-%d INFO Checking survivors\n",
                 System.currentTimeMillis(),
                 this.id,
                 this.id);
         
        if (alivesReceived.size() > 0) {
            /*
             * here the view must be changed. A node crashed. New
             * members are current members minus the ones from which the
             * heartbeat has not been received.
             */
            Set<ActorRef> newView = new HashSet<>(
                    this.tempView.members);

            // ----------------------------------
            // This is just to have additional info
            // on crashed nodes. It's of no other use.
            List<String> nodesCrashed = new ArrayList<>();
            int pid = 0;
            for (ActorRef actor : alivesReceived) {
                newView.remove(actor);
                pid = this.aliveProcesses.getIdByActor(actor);
                nodesCrashed.add("p" + ((Integer) pid).toString());
                onCrashedProcess(actor);
            }
            System.out.printf("%d P-%d P-%d INFO nodes: %s crashed.\n",
                    System.currentTimeMillis(),
                    this.id,
                    this.id,
                    nodesCrashed.toString());
            // ----------------------------------
            alivesReceived.clear();
            onViewChange(newView);
        } else {
            HashSet<ActorRef> participants =
                    new HashSet<>(this.tempView.members);
            participants.remove(this.getSelf()); // exclude the group
                                                 // manager

            for (ActorRef participant : participants) {
                alivesReceived.add(participant);
                sendNetworkMessage(new AliveMsg(this.aliveId, this.id),
                        participant);
            }
            aliveId++;
        }
        sendTimeoutMessage(new CheckViewMsg());
    }

    private void onAliveMsg(AliveMsg msg) {
        alivesReceived.remove(this.getSender());
        
         //DEBUG:
         System.out.printf("%d P-%d P-%d received_alive_message %s\n",
                 System.currentTimeMillis(), this.id, msg.senderID,
                 msg.toString());
    }

    /**
     * An inverse heartbeat flowing from each participant to the group
     * manager.
     *
     * When we terminate the group manager, the this won't answer to
     * heartbeat request anymore, thus participant can terminate.
     *
     * @param msg
     */
    private void onGmAliveMsg(GmAliveMsg msg) {
        sendNetworkMessage(new GmAliveMsg(),
                this.getSender());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinRequestMsg.class, this::onJoinRequestMsg)
                .match(StopMulticastMsg.class, this::onStopMulticast)
                .match(ViewChangeMsg.class, this::onViewChangeMsg)
                .match(FlushMsg.class, this::onFlushMsg)
                .match(SendMulticastMsg.class, this::onSendMulticastMsg)
                .match(Message.class, this::onReceiveMessage)
                .match(CheckViewMsg.class, this::onCheckViewMsg)
                .match(AliveMsg.class, this::onAliveMsg)
                .match(GmAliveMsg.class, this::onGmAliveMsg)
                // handle (receiving) the step message defined in
                // the EventsController
                .match(SendStepMsg.class, this::onSendStepMsg)
                .build();
    }
}
