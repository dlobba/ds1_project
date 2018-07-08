package reliable_multicast.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import reliable_multicast.EventsController;
import reliable_multicast.EventsController.Event;

public class Config {

	private boolean manual_mode;
	private Map<String, Map<Event, Set<String>>> events;
	private Map<Integer, Set<String>> senders;
	private Map<Integer, Set<String>> risen;
	private Map<Integer, Set<String>> views;
		
	public Config() {
		this.manual_mode = false;
		this.events = new HashMap<>();
		this.senders = new HashMap<>();
		this.risen = new HashMap<>();
		this.views = new HashMap<>();
	}
	
	public boolean isManual_mode() {
		return manual_mode;
	}
	
	public void setManual_mode(boolean manual_mode) {
		this.manual_mode = manual_mode;
	}
	
	public Map<String, Map<Event, Set<String>>> getEvents() {
		Map<String, Map<Event, Set<String>>> events = new HashMap<>();
		Iterator<String> msgLabelIterator =
				events.keySet().iterator();
		String msgLabel = null;
		Map<Event, Set<String>> tmpMap;
		while (msgLabelIterator.hasNext()) {
			msgLabel = msgLabelIterator.next();
			tmpMap = new HashMap<>();
			tmpMap.putAll(this.events.get(msgLabel));
			events.put(msgLabel, tmpMap);
		}
		return this.events;
	}
	
	public Map<String, Map<Event, Set<String>>> getUnderlyingEvents() {
		return this.events;
	}
	
	public Map<Integer, HashSet<String>> getSenders() {
		Map<Integer, HashSet<String>> tmp =
				new HashMap<Integer, HashSet<String>>();
		Integer tmpKey;
		Iterator<Integer> keyIterator =
				this.senders.keySet().iterator();
		while (keyIterator.hasNext()) {
			tmpKey = keyIterator.next();
			tmp.put(tmpKey,
					new HashSet<>(this.senders.get(tmpKey)));
		}
		return tmp;
	}
	
	public Map<Integer, Set<String>> getUnderlyingSenders() {
		return this.senders;
	}
	
	public Map<Integer, HashSet<String>> getRisen() {
		Map<Integer, HashSet<String>> tmp =
				new HashMap<Integer, HashSet<String>>();
		Integer tmpKey;
		Iterator<Integer> keyIterator =
				this.risen.keySet().iterator();
		while (keyIterator.hasNext()) {
			tmpKey = keyIterator.next();
			tmp.put(tmpKey,
					new HashSet<>(this.risen.get(tmpKey)));
		}
		return tmp;
	}
	
	public Map<Integer, Set<String>> getUnderlyingRisen() {
		return this.risen;
	}

	public Map<Integer, HashSet<String>> getViews() {
		Map<Integer, HashSet<String>> tmp =
				new HashMap<Integer, HashSet<String>>();
		Integer tmpKey;
		Iterator<Integer> keyIterator =
				this.views.keySet().iterator();
		while (keyIterator.hasNext()) {
			tmpKey = keyIterator.next();
			tmp.put(tmpKey,
					new HashSet<>(this.views.get(tmpKey)));
		}
		return tmp;
	}
	
	public Map<Integer, Set<String>> getUnderlyingViews() {
		return this.views;
	}
}
