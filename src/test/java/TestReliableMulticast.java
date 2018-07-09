import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import junit.framework.TestSuite;
import reliable_multicast.GroupManager;
import reliable_multicast.Participant;


public class TestReliableMulticast extends TestSuite {
	
	static ActorSystem system;
	static TestActorRef<GroupManager> groupManagerAr;
	
	@Before
	public void initializeSystem() {
		system = ActorSystem.create();
		groupManagerAr = TestActorRef.create(system,
				GroupManager.props(0),
				"gm");
	}
	
	@After
	public void cleanSystem() {
		TestKit.shutdownActorSystem(system);
		system = null;
	}

	/**
	 * Create a new participant and check
	 * whether the idPool of the group manager
	 * is increased and the participant has been
	 * given the last value of the pool.
	 */
	@Test
	public void testJoin() {
		
		final GroupManager groupManager = groupManagerAr
				.underlyingActor();
		
		new TestKit(system) {{
			
			System.out.println(groupManager.getIdPool());
			assertTrue(groupManager.getIdPool() == 1);
			
			int nextId = groupManager.getIdPool();
			
			final TestActorRef<Participant> part1 = TestActorRef
					.create(system,
							Participant.props(groupManagerAr),
							"p1");
			// TODO: add small delay before checking
			assertTrue(part1.underlyingActor().getId() ==
					nextId);
			assertTrue(groupManager.getIdPool() ==  nextId + 1);
			assertTrue(groupManager.getView()
					.getMembers()
					.contains(part1));
		}};
		
	}
	
}
