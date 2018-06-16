package reliable_multicast.messages;

import java.io.Serializable;

public class Message implements Serializable {
	// id is made of sender pid + message id
	// so we consider total order
	public final int senderID;
	public final int messageID;
	public final boolean stable;
	
	public Message(int pID, int mID, boolean stable) {
		this.senderID = pID;
		this.messageID = mID;
		this.stable = stable;
	}
	
	public Message(Message message, boolean stable) {
		this.senderID = message.senderID;
		this.messageID = message.messageID;
		this.stable = stable;
	}

	/*
	 * Only the message and sender ID are compared.
	 */
	@Override
	public boolean equals(Object other) {
		if (other == null)
			return false;
		if (this == other)
			return true;
		
		if (other instanceof Message) {
			return (this.messageID == ((Message) other).messageID &&
					this.senderID == ((Message) other).senderID);
		}
		return false;
	}

	@Override
	public String toString() {
		return "p" + senderID + "m" + messageID;
	}
};