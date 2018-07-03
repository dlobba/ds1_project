package test_messages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import reliable_multicast.messages.Message;

public class TestMessage {
	
	@Test
	public void testMessageEqualityExtremes() {
		Message m1 = new Message(0, 1, false);
		assertNotEquals(m1, null);
		assertEquals(m1, m1);	
	}
	
	@Test
	public void testMessageIdentity() {
		Message m1 = new Message(0, 1, false);
		Message m2 = new Message(0, 1, false);
		assertEquals(m1, m2);
	}
	
	@Test
	public void testMessageEqualsId() {
		Message m1 = new Message(0, 1, false);
		Message m2 = new Message(m1, true);
		assertEquals(m1, m2);
	}
	
	@Test
	public void testMessageNotEquals() {
		Message m1 = new Message(0, 1, false);
		Message m2 = new Message(0, 2, false);
		assertNotEquals(m1, m2);
	}
	
	@Test
	public void testHashSet() {
		Message m1 = new Message(0, 1, false);
		Message m2 = new Message(0, 1, false);
		
		Set<Message> hs = new HashSet<>();
		hs.add(m1);
		assertTrue(hs.contains(m2));
		int previousSize = hs.size();
		// the stable attribute is not
		// considered by the 'equals' method
		hs.add(new Message(m2, true));
		assertTrue(hs.size() == previousSize);
	}

}
