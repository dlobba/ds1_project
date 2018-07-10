package reliable_multicast.messages;

import java.io.Serializable;

public class Message implements Serializable {
	// id is made of sender pid + message id
	// so we consider total order
	public final int senderID;
	public final int messageID;
	public final boolean stable;
	public final int viewId;
	
	public Message(int pID, int mID, int viewId, boolean stable) {
		this.senderID = pID;
		this.messageID = mID;
		this.viewId = viewId;
		this.stable = stable;
	}
	
	public Message(Message message, boolean stable) {
		this.senderID = message.senderID;
		this.messageID = message.messageID;
		this.viewId = message.viewId;
		this.stable = stable;
	}

	// this method is required for hash sets
	// to work
	public String getLabel() {
		return "p" + senderID + "m" + messageID;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + messageID;
		result = prime * result + senderID;
		result = prime * result + viewId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Message))
			return false;
		Message other = (Message) obj;
		if (messageID != other.messageID)
			return false;
		if (senderID != other.senderID)
			return false;
		if (viewId != other.viewId)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "p" + senderID + "m"
				+ messageID + ":v" + this.viewId;
	}
};