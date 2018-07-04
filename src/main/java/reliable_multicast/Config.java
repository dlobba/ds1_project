package reliable_multicast;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import reliable_multicast.EventsController.Event;

public class Config {

	private boolean manual_mode;
	private Map<String, Event> events;
	private Map<Integer, Set<String>> steps;
		
	public Config() {
		this.manual_mode = false;
		this.events = new HashMap<>();
		this.steps = new HashMap<>();
	}
	
	public boolean isManual_mode() {
		return manual_mode;
	}
	
	public void setManual_mode(boolean manual_mode) {
		this.manual_mode = manual_mode;
	}
	
	public Map<String, Event> getEvents() {
		Map<String, Event> events = new HashMap<>();
		Iterator<String> msgLabelIterator =
				events.keySet().iterator();
		String msgLabel = null;
		while (msgLabelIterator.hasNext()) {
			msgLabel = msgLabelIterator.next();
			events.put(msgLabel, this.events.get(msgLabel));
		}
		return this.events;
	}
	
	public Map<String, Event> getUnderlyingEvents() {
		return this.events;
	}
	
	public Map<Integer, HashSet<String>> getSteps() {
		Map<Integer, HashSet<String>> tmp =
				new HashMap<Integer, HashSet<String>>();
		Integer tmpKey;
		Iterator<Integer> keyIterator =
				this.steps.keySet().iterator();
		while (keyIterator.hasNext()) {
			tmpKey = keyIterator.next();
			tmp.put(tmpKey,
					new HashSet<>(this.steps.get(tmpKey)));
		}
		return tmp;
	}
	
	public Map<Integer, Set<String>> getUnderlyingSteps() {
		return this.steps;
	}

}
