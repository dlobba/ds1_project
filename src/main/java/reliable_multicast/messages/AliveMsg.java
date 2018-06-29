package reliable_multicast.messages;

import java.io.Serializable;
public class AliveMsg implements Serializable {
	
	public final int messageID;
	public final int senderID;
	
	public AliveMsg(int messageID, int senderID) {
		this.messageID = messageID;
		this.senderID = senderID;
	}
	
	public String toString() {
		return "p" + senderID + "a" + messageID;
	}
};
