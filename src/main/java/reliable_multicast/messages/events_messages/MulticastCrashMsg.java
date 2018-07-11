package reliable_multicast.messages.events_messages;
import java.io.Serializable;

public class MulticastCrashMsg extends EventMessage
implements Serializable {

	public enum MutlicastCrashType {
		MULTICAST_N_CRASH,
		MULTICAST_ONE_N_CRASH,
	}
	public final MutlicastCrashType type;
	
	public MulticastCrashMsg(MutlicastCrashType type) {
		this.type = type;
	};
}