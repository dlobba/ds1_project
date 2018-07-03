package reliable_multicast.messages.crash_messages;
import java.io.Serializable;

public class ReceivingCrashMsg extends CrashMessage
implements Serializable{

	public enum ReceivingCrashType {
		RECEIVE_VIEW_N_CRASH,
		RECEIVE_MULTICAST_N_CRASH
	}
	public final ReceivingCrashType type;

	public ReceivingCrashMsg(ReceivingCrashType type) {
		this.type = type;
	}
}
