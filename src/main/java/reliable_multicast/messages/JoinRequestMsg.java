package reliable_multicast.messages;

import java.io.Serializable;

public class JoinRequestMsg implements Serializable {
    // this kind of message is also used when the Group
    // Manager assigns an ID to the caller
    public final int idAssigned;

    public JoinRequestMsg(int idAssigned) {
        this.idAssigned = idAssigned;
    }

    public JoinRequestMsg() {
        this(0);
    }
};
