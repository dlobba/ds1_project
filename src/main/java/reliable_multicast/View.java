package reliable_multicast;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import akka.actor.ActorRef;

public class View {
    int id;
    Set<ActorRef> members;
    Set<Integer> membersIds;

    public View(int id, Set<ActorRef> members, Set<Integer> membersIds) {
        this.id = id;
        this.members = new HashSet<>(members);
        this.membersIds = new HashSet<>(membersIds);
    }

    public View(int id) {
        this(id, new HashSet<>(), new HashSet<>());
    }

    public View(View other) {
        this.id = other.id;
        this.members = new HashSet<>(other.members);
        this.membersIds = new HashSet<>(other.membersIds);
    }

    public int getId() {
        return id;
    }

    public Set<ActorRef> getMembers() {
        return new HashSet<>(members);
    }

    public Set<Integer> getMembersIds() {
        return new HashSet<>(membersIds);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (obj instanceof View) {
            View other = (View) obj;
            if (id != other.id)
                return false;
            if (members == null) {
                if (other.members != null)
                    return false;
            }
            if (!(members.containsAll(other.members) && other.members
                    .containsAll(members)))
                return false;
        }
        return true;
    }

    /**
     * Return a comma separated list of IDs related
     * to participants in the current view.
     *
     * @return
     */
    public String logMembers() {
        List<String> membersString = new ArrayList<>();
        for (Integer member : membersIds) {
            membersString.add(member.toString());
        }
        return String.join(",", membersString);
    }

    @Override
    public String toString() {
        List<String> membersString = new ArrayList<>();
        for (ActorRef member : members) {
            membersString.add(member.path().name());
        }
        return "V" + id + "={" +
                String.join(", ", membersString) + "}";
    }
}
