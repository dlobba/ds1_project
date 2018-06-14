package reliable_multicast.messages;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import akka.actor.ActorRef;
import reliable_multicast.View;

public class ViewChangeMsg implements Serializable {
	// TODO: change to private and add get-setters
	public final View view;
	public ViewChangeMsg(View view) {
		this.view = new View(view);
	}
};
