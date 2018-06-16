package reliable_multicast;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import akka.actor.ActorRef;

public class View {
	int id;
	Set<ActorRef> members;
	
	public View(int id, Set<ActorRef> members) {
		this.id = id;
		this.members = new HashSet<>(members);
	}
	
	public View(int id) {
		this(id, new HashSet<>());
	}
	
	public View(View other) {
		this.id = other.id;
		this.members = new HashSet<>(other.members);
	}

	@Override
	public String toString() {
		List<String> membersString = new ArrayList<>();
		for (ActorRef member : members) {
			membersString.add(member.path().name());
		}
		return "V" + id + "={" +
			String.join(", ", membersString)
			+ "}";
	}
}
