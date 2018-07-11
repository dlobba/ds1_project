package reliable_multicast.messages;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import akka.actor.ActorRef;
import reliable_multicast.View;

public class ViewChangeMsg implements Serializable {
    public final int id;
    public final Set<ActorRef> members;

    public ViewChangeMsg(View view) {
        this.id = view.getId();
        this.members = Collections.unmodifiableSet(view.getMembers());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + ((members == null) ? 0 : members.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof ViewChangeMsg))
            return false;
        ViewChangeMsg other = (ViewChangeMsg) obj;
        if (id != other.id)
            return false;
        if (members == null) {
            if (other.members != null)
                return false;
        } else if (!members.equals(other.members))
            return false;
        return true;
    }
};
