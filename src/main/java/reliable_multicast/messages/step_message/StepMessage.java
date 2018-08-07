package reliable_multicast.messages.step_message;

import java.io.Serializable;

public class StepMessage implements Serializable {
	
	public int id;
	
	public StepMessage(int id) {
		this.id = id;
	}
	
}
