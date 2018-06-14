package reliable_multicast.messages;

import java.io.Serializable;

public class Message implements Serializable {
	private final String id;
	private final boolean stable;
	
	public Message(String id, boolean stable) {
		this.id = id;
		this.stable = stable;
	}
};