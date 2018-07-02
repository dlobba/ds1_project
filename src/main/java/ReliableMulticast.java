import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import reliable_multicast.GroupManager;
import reliable_multicast.Participant;
import reliable_multicast.messages.CrashMsg;
import reliable_multicast.messages.JoinRequestMsg;
import reliable_multicast.messages.ReviveMsg;
import reliable_multicast.BaseParticipant.SendMulticastMsg;
import scala.concurrent.duration.Duration;

public class ReliableMulticast {
	
	final static int N_NODES = 2;
	
	public static void main(String[] args) {
		// Create the actor system
		System.out.print("Reliable multicast started!\n");
	    final ActorSystem system = ActorSystem.create("multicast_system");
	    
	    final ActorRef groupManager = system.actorOf(GroupManager.props(0),
	    		"gm");
//		Maybe this can be invoked when the GM is initiated...
//	    groupManager.tell(new CheckViewMsg(), null);
//	    
	    final ActorRef p1 = system.actorOf(Participant.props(groupManager),
	    		"p1");
	    final ActorRef p2 = system.actorOf(Participant.props(groupManager),
	    		"p2");
	    
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
			null);
	}

}
