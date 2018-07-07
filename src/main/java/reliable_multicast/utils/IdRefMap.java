package reliable_multicast.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import akka.actor.ActorRef;

/**
 * Shared behavior for map objects, namely
 * among aliveProcesses and crasshedProcesses.
 * Their methods are the same, so we
 * made the same code and put it into this
 * class.
 */
public class IdRefMap {
	private Map<Integer, ActorRef> map;
	
	public IdRefMap() {
		super();
		this.map = new HashMap<>();
	}
	
	public Set<Integer> getProcessesIds() {
		return this.map.keySet();
	}
	
	public Set<ActorRef> getProcessesActors() {
		return new HashSet<>(this.map.values());
	}

	/**
	 * An element can be added iif it's unique
	 * both to the keys set and to the values set.
	 * 
	 * The association must be bijective.
	 * 
	 * @param id
	 * @param actor
	 * @return
	 */
	public ActorRef addIdRefAssoc(Integer id,
								   ActorRef actor) {
		if (map.containsKey(id))
			return null;
		if (map.containsValue(actor))
			return null;
		return map.put(id, actor);
	}
	
	public ActorRef getActorById(Integer id) {
		return map.get(id);
	}
	
	public Integer getIdByActor(ActorRef actor) {
		Iterator<Integer> keyIter = map.keySet().iterator();
		Integer tmpKey = null;
		while (keyIter.hasNext()) {
			tmpKey = keyIter.next();
			if (map.get(tmpKey).equals(actor))
				return tmpKey;
		}
		return null;
	}
	
	public void removeIdRefEntry(Integer id) {
		map.remove(id);
	}
}
