package reliable_multicast.messages;

import java.io.Serializable;

import akka.actor.ActorRef;

public class FlushMsg implements Serializable {
    public final int senderID;
    public final int viewID;
    public final ActorRef sender;

    public FlushMsg(int pID, int vID, ActorRef sender) {
        this.senderID = pID;
        this.viewID = vID;
        this.sender = sender;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((sender == null) ? 0 : sender
                .hashCode());
        result = prime * result + senderID;
        result = prime * result + viewID;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof FlushMsg))
            return false;
        FlushMsg other = (FlushMsg) obj;
        if (sender == null) {
            if (other.sender != null)
                return false;
        } else if (!sender.equals(other.sender))
            return false;
        if (senderID != other.senderID)
            return false;
        if (viewID != other.viewID)
            return false;
        return true;
    }
};
