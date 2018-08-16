import java.io.File;
import java.io.FileReader;
import com.google.gson.Gson;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import reliable_multicast.GroupManager;
import reliable_multicast.Participant;

public class ReliableMulticast {

    public final static String SYSTEM_NAME = "multicast_system";
    public final static String GROUP_MANAGER_NAME = "gm";
    public final static String PARTICIPANT_NAME = "part";

    private static boolean hasParticipantProperties(Config config) {
        if (!config.hasPath("participant.remote_ip"))
            return false;
        if (!config.hasPath("participant.remote_port"))
            return false;
        if (!config.hasPath("participant.id"))
            return false;
        return true;
    }

    /*
     * The main acts as an initiator for a single node.
     * 
     * It can be called in two different ways:
     *   1. specifying a remote akka config file
     *   2. specifying a json event config file
     *   
     * The first file is required to instantiate
     * the parameters required for the actor to work.
     * 
     * If it defines a the is-manager properties the
     * node initiated is a group manager, otherwise
     * it's a normal participant.
     * 
     * The first node to be created should
     * be the group manager.
     * 
     * The second file defines the events file to be used
     * in the system. All nodes should call the same events
     * file in order for the execution to be meaningful.
     */
    public static void main(String[] args) {
        String eventsFileName = System.getProperty("events");
        String configFilePath = System.getProperty("config.resource");
        String resourcesDir = System.getProperty("project_dir") +
                "/src/main/resources/";

        /*
         * Check for file existence, parse errors, load
         * conf files.
         */
        if (configFilePath.trim().isEmpty()) {
            System.err.println("\n[ERROR]: No akka config resource defined." +
                    " TERMINATING...");
            System.exit(-1);
        }
        // Load configuration file
        Config config = ConfigFactory.load();

        reliable_multicast.utils.Config eventsConf =
                new reliable_multicast.utils.Config();
        if (!eventsFileName.trim().isEmpty()) {
            String eventsFilePath = resourcesDir +
                    eventsFileName.trim();

            System.out.printf("Reading events from:\n\t%s\n",
                    eventsFilePath);
            try {
                Gson gson = new Gson();
                FileReader fr;
                fr = new FileReader(new File(eventsFilePath));
                eventsConf = gson.fromJson(fr, reliable_multicast
                        .utils
                        .Config.class);
                System.out.println(gson.toJson(eventsConf));
                fr.close();
            } catch (Exception e) {
                System.out.println("\n[ERROR]: " +
                        e.getMessage() +
                        "\nTERMINATING...");
                System.exit(-1);
            }
        }
        
        boolean isManager = false;
        boolean isParticipant = hasParticipantProperties(config);
        if (config.hasPath("participant.is_manager")) {
            isManager = config.getBoolean("participant.is_manager");
        }
        if (! (isManager || isParticipant)) {
            /*
             * Invalid config file, it doesn't
             * represent neither a group manager
             * nor a participant.
             */
            System.err.println("\n[ERROR]: Invalid config file.\n" +
                    "TERMINATING...");
            System.exit(-1);
        }

        final ActorSystem system = ActorSystem.create(SYSTEM_NAME, config);
        if (isManager) {
            // create group manager
            system.actorOf(GroupManager.props(0,
                    eventsConf.isManual_mode(),
                    eventsConf.getUnderlyingEvents(),
                    eventsConf.getUnderlyingSenders(),
                    eventsConf.getUnderlyingRisen(),
                    eventsConf.getUnderlyingViews()),
                    GROUP_MANAGER_NAME);
        } else {
            String remote_ip = config.getString("participant.remote_ip");
            String remote_port = config.getString("participant.remote_port");
            Integer participant_id = config.getInt("participant.id");
            String remotePath = "akka.tcp://" + SYSTEM_NAME +
                    "@" + remote_ip + ":" + remote_port +
                    "/user/" + GROUP_MANAGER_NAME;
            system.actorOf(Participant.props(remotePath,
                    eventsConf.isManual_mode()),
                    PARTICIPANT_NAME +
                    participant_id.toString());
        }
        System.out.print("Reliable multicast started!\n");
    }
}
