package reliable_multicast.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import reliable_multicast.EventsController;
import reliable_multicast.EventsController.Event;

public class EventsList {

	private final Map<String, Event> events;
	private final Map<Integer, Integer> processLastCall;
	public EventsList() {
		super();
		this.events = new HashMap<>();
		this.processLastCall = new HashMap<>();
	}
	
	public String getProcessLabel(Integer processId) {
		Integer messageId = this.processLastCall.get(processId);
		if (messageId == null)
			return null;
		return "p" + processId.toString() +
			   "m" + messageId.toString();
	}
	
	public String getProcessNextLabel(Integer processId) {
		Integer messageId = this.processLastCall.get(processId);
		if (messageId == null)
			return null;
		messageId += 1;
		return "p" + processId.toString() +
			   "m" + messageId.toString();
	}
	
	public void updateProcessLastCall(Integer processId, Integer messageID) {
		Integer oldEntry = this.processLastCall.get(processId);
		if (oldEntry == null)
			return;
		if (oldEntry >= messageID)
			return;
		this.processLastCall.put(processId, messageID);
	}
	
	/**
	 * Return true if the event associated
	 * to the label implies sending a normal
	 * message. If it doesn't or no event is
	 * associated then return false.
	 * 
	 * @param eventLabel
	 * @return
	 */
	public boolean isSendingEvent(String eventLabel) {
		Event event = this.events.get(eventLabel);
		if (event == null)
			return false;
		if (event == Event.MULTICAST_N_CRASH ||
			event == Event.MULTICAST_ONE_N_CRASH)
			return true;
		return false;
	}
	
	public Event getEvent(String eventLabel) {
		return this.events.get(eventLabel);
	}
	
	public void fromMap(Map<String, Event> events) {
		if (events == null)
			return;

		String tmpLabel;
		Pattern labelPattern = Pattern.compile("^p([0-9]+)m([0-9]+)$");
		
		Iterator<String> labelIterator =
					events.keySet().iterator();
		while (labelIterator.hasNext()) {
			tmpLabel = labelIterator.next();
			Matcher labelMatcher = labelPattern.matcher(tmpLabel);
			if (labelMatcher.matches()) {
				this.events.put(tmpLabel,
						events.get(tmpLabel));
				/*
				 *  Add the process to the map.
				 *  We need to track this process.
				 */
				this.processLastCall
					.put(Integer.parseInt(labelMatcher.group(1)),
										 -1);
			}
		}
	}
	
	
	
}
