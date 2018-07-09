import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import reliable_multicast.GroupManager;
import reliable_multicast.Participant;
import reliable_multicast.messages.CrashMsg;
import scala.concurrent.duration.Duration;

public class ReliableMulticast {
	
	final static int N_NODES = 2;
	
	public static void main(String[] args) {
		// Load configuration file
		Config config = ConfigFactory.load();
		int id = config.getInt("nodeapp.id");
		String remote_ip = config.getString("nodeapp.remote_ip");
		int nodePort = config.getInt("nodeapp.remote_port");
		int isManager = config.getInt("nodeapp.isManager");
		
		// Create the actor system
		System.out.print("Reliable multicast started!\n");
	    final ActorSystem system = ActorSystem.create("multicast_system", config);
	    
	    if(isManager == 1) {
	    	final ActorRef groupManager = system.actorOf(GroupManager.props(0),
		    		"gm");
	    } else {
	    	final ActorRef participant = system.actorOf(Participant.props(),
		    		"p" + Integer.toString(id));
	    }
	}
}
