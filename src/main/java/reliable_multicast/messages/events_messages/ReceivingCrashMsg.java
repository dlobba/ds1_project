package reliable_multicast.messages.events_messages;

import java.io.Serializable;

public class ReceivingCrashMsg extends EventMessage implements Serializable {
    public final String eventLabel;

    public enum ReceivingCrashType {
        RECEIVE_VIEW_N_CRASH,
        RECEIVE_MULTICAST_N_CRASH
    }

    public final ReceivingCrashType type;

    public ReceivingCrashMsg(
        ReceivingCrashType type,
        String eventLabel) {
        this.type = type;
        this.eventLabel = eventLabel;
    }
}
