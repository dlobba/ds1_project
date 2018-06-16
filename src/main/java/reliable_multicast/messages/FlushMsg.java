package reliable_multicast.messages;

import java.io.Serializable;

public class FlushMsg implements Serializable {
	public final int senderID;
	public final int viewID;
	
	public FlushMsg(int pID, int vID) {
		this.senderID = pID;
		this.viewID = vID;
	}
};
