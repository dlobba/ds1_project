package reliable_multicast.messages;

import java.io.Serializable;
import reliable_multicast.View;

public class ViewChangeMsg implements Serializable {
	// TODO: change to private and add get-setters
	public final View view;
	public ViewChangeMsg(View view) {
		this.view = new View(view);
	}
	@Override
	public boolean equals(Object other) {
		if (other == null)
			return false;
		if (this == other)
			return true;
		
		if (other instanceof ViewChangeMsg) {
			return (this.view
					.equals(((ViewChangeMsg) other).view));
		}
		return false;
	}
};
