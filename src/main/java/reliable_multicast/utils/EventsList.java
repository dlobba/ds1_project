package reliable_multicast.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import reliable_multicast.EventsController.Event;

public class EventsList {

    private final Map<String, Map<Event, Set<Integer>>> events;

    public EventsList() {
        super();
        this.events = new HashMap<>();
    }

    /**
     * Return true if the event associated to the label implies sending
     * a normal message. If it doesn't or no event is associated then
     * return false.
     * 
     * @param eventLabel
     * @return
     */
    public boolean isSendingEvent(String eventLabel) {
        Map<Event, Set<Integer>> eventList =
                this.events.get(eventLabel);
        if (eventList == null)
            return false;
        Event event = eventList.keySet()
                .iterator()
                .next();
        if (event == null)
            return false;
        if (event == Event.MULTICAST_N_CRASH ||
                event == Event.MULTICAST_ONE_N_CRASH)
            return true;
        return false;
    }

    public Event getEvent(String eventLabel) {
        Map<Event, Set<Integer>> eventList =
                this.events.get(eventLabel);
        if (eventList == null)
            return null;
        Event event = eventList.keySet()
                .iterator()
                .next();
        return event;
    }

    /**
     * Given the event label, return the list of processes that are
     * targeted by the event.
     * 
     * @param eventLabel
     * @return
     */
    public Set<Integer> getEventReceivers(String eventLabel) {
        Map<Event, Set<Integer>> eventList =
                this.events.get(eventLabel);
        if (eventList == null)
            return new HashSet<>();
        Event event = eventList.keySet()
                .iterator()
                .next();
        return eventList.get(event);
    }

    /**
     * Parse event strings and return the list of
     * processes involved.
     * @param events
     */
    public Set<Integer> fromMap(Map<String, Map<Event, Set<String>>> events) {
        if (events == null)
            return new HashSet<>();
        Set<Integer> processes = new HashSet<>();

        String tmpLabel;
        Pattern labelPattern = Pattern.compile("^p([0-9]+)m([0-9]+)$");
        Iterator<String> labelIterator =
                events.keySet().iterator();
        Map<Event, Set<Integer>> tmpEventProcessMap;
        while (labelIterator.hasNext()) {
            tmpLabel = labelIterator.next();
            Matcher labelMatcher = labelPattern.matcher(tmpLabel);
            if (labelMatcher.matches()) {
                tmpEventProcessMap = this.eventProcessFromMap(events
                        .get(tmpLabel));
                if (tmpEventProcessMap != null)
                    this.events.put(tmpLabel, tmpEventProcessMap);
                /*
                 * Add the process to the map.
                 * We need to track this process.
                 */
                processes.add(Integer.parseInt(labelMatcher.group(1)));
            }
        }
        return processes;
    }

    public Map<Event, Set<Integer>> eventProcessFromMap(
            Map<Event, Set<String>> processMap) {
        if (processMap == null)
            return null;

        Pattern labelPattern = Pattern.compile("^p([0-9]+)$");
        Iterator<Event> eventIterator = processMap.keySet().iterator();
        if (!eventIterator.hasNext())
            return null;

        Event event = eventIterator.next();
        // the event is meaningful
        if (event == null) {
            System.out.println("UNKNOWN EVENT - it will be ignored.");
            return null;
        }

        Iterator<String> processIterator =
                processMap.get(event)
                        .iterator();

        Set<Integer> processes = new HashSet<>();
        String tmpProcess;
        Matcher processMatch;
        while (processIterator.hasNext()) {
            tmpProcess = processIterator.next();
            processMatch = labelPattern.matcher(tmpProcess);
            if (processMatch.matches()) {
                processes.add(
                        Integer.parseInt(processMatch.group(1)));
            }
        }
        Map<Event, Set<Integer>> tmpMap = new HashMap<>();
        tmpMap.put(event, processes);
        return tmpMap;
    }
}
