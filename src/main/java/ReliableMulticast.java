import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import reliable_multicast.GroupManager;
import reliable_multicast.Participant;
import reliable_multicast.messages.CrashMsg;
import reliable_multicast.messages.JoinRequestMsg;
import reliable_multicast.messages.ReviveMsg;
import reliable_multicast.utils.Config;
import reliable_multicast.BaseParticipant.SendMulticastMsg;
import reliable_multicast.EventsController.Event;
import scala.concurrent.duration.Duration;

public class ReliableMulticast {
	
	final static int N_NODES = 2;
	
	public static void main(String[] args) {
		
		Map<String, Event> events = new HashMap<String, Event>();
		events.put("p1m5", Event.MULTICAST_ONE_N_CRASH);
		
		
		Gson gson = new Gson();
		
		try {
			FileReader fr;
			fr = new FileReader("/home/lubuntu/Desktop/test1.json");

			Config conf1 = gson.fromJson(fr, Config.class);
			System.out.println(gson.toJson(conf1));
			fr.close();
		
		
		// Create the actor system
		System.out.print("Reliable multicast started!\n");
	    final ActorSystem system = ActorSystem.create("multicast_system");
	    
	    final ActorRef groupManager =
	    		system.actorOf(GroupManager.props(0,
	    										  conf1.isManual_mode(),
	    										  conf1.getUnderlyingEvents(),
	    										  conf1.getUnderlyingSenders(),
	    										  conf1.getUnderlyingRisen(),
	    										  conf1.getUnderlyingViews()),
	    					   "gm");
//		Maybe this can be invoked when the GM is initiated...
//	    groupManager.tell(new CheckViewMsg(), null);
//	    
	    final ActorRef p1 =
	    		system.actorOf(Participant.props(groupManager,
	    										 conf1.isManual_mode()),
	    					   "p1");
	    final ActorRef p2 =
	    		system.actorOf(Participant.props(groupManager,
	    										 conf1.isManual_mode()),
	    					   "p2");
	    
	    /*
	    system.scheduler().scheduleOnce(Duration.create(20,
				TimeUnit.SECONDS),
			p2,
			new CrashMsg(),
			system.dispatcher(),
			null);
	    system.scheduler().scheduleOnce(Duration.create(25,
				TimeUnit.SECONDS),
			p2,
			new ReviveMsg(),
			system.dispatcher(),
			null);*/
	    
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
