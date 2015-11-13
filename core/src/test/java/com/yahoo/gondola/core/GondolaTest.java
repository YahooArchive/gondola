/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.core;

import com.yahoo.gondola.Command;
import com.yahoo.gondola.Member;
import com.yahoo.gondola.Role;
import com.yahoo.gondola.RoleChangeEvent;
import com.yahoo.gondola.rc.GondolaRc;
import com.yahoo.gondola.rc.MemberRc;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * This is the TestNG suite of Gondola unit tests.
 * See conf/gondola-rc.conf to see how the gondola instances are configured.
 * All the unit tests assume a three-node cluster configuration.
 *
 * The test makes extensive use of GondolaRc and MemberRc objects (rc stands for remote control).
 * These objects are wrappers around the regular Gondola and Member instances and provides methods 
 * that make it easier to set up a test case.
 * For example, you can use MemberRc to initialize the Raft log of a particular member with a few entries.
 */
public class GondolaTest {
    static final Logger logger = LoggerFactory.getLogger(GondolaTest.class);
    GondolaRc gondolaRc;

    // The members are 
    MemberRc member1;
    MemberRc member2;
    MemberRc member3;
    List<MemberRc> members = new ArrayList<>();

    // Set to now at the start of a test case
    long startTimer;

    // When > 0, causes the virtual clock to tick at the specified value (in ms)
    int runningTick;

    // If non-null, the name of the currently running test
    String currentTest;

    // When non-null, the main thread will throw this exception at the earliest opportunity
    Throwable exceptionInAnotherThread;

    public GondolaTest() throws Exception {
        PropertyConfigurator.configure("conf/gondola-rc.log4j.properties");
        gondolaRc = new GondolaRc();
        member1 = gondolaRc.getMember(1);
        member2 = gondolaRc.getMember(2);
        member3 = gondolaRc.getMember(3);
        members = Stream.of(gondolaRc.getMember(1), gondolaRc.getMember(2), gondolaRc.getMember(3))
                .collect(Collectors.toList());
        startTimer = System.currentTimeMillis();
        new SummaryThread().start();
    }

    /**
     * Starts all the threads in the GondolaRc instance and displays a
     * header in the log, to make it easier to find the output of a
     * test case.
     */
    @BeforeMethod(alwaysRun = true)
    public void doBeforeMethod(ITestContext tc, ITestResult tr, Method m) throws Exception {
        final String mname = m.getName();
        currentTest = this.getClass().getName() + "." + mname;
        logger.info(String.format("************************ %s *******************", currentTest));
        gondolaRc.start();
    }

    /**
     * Stops all the therads in the GondolaRc instance and exits if the test case failed.
     */
    @AfterMethod(alwaysRun = true)
    public void doAfterMethod(ITestContext tc, ITestResult tr, Method m) throws Exception {
        runningTick = 0;

        if (tr.getStatus() != ITestResult.SUCCESS) {
            Throwable t = tr.getThrowable();
            if (t != null) {
                logger.error("Test case failed.\n\n" + t.getMessage(), t);
                System.exit(1);
            }
        }
        if (exceptionInAnotherThread != null) {
            logger.error("Test case failed.\n\n" + exceptionInAnotherThread.getMessage(), exceptionInAnotherThread);
            System.exit(1);
        }
        gondolaRc.stop();
    }

    /**
     * Convenience method to commit a string to a particular member.
     */
    void commit(MemberRc member, String s) {
        try {
            Command command = member.checkoutCommand();
            byte[] bytes = s.getBytes("UTF-8");
            command.commit(bytes, 0, bytes.length);
            command.release();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Commit in a thread.
     *
     * @param delay The command will committed after delay milliseconds.
     */
    CompletableFuture commitAsync(MemberRc member, String s, int delay) {
        return CompletableFuture.runAsync(() -> {
            try {
                gondolaRc.sleep(delay);
                Command command = member.checkoutCommand();
                byte[] bytes = s.getBytes("UTF-8");
                command.commit(bytes, 0, bytes.length);
                command.release();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * @param term if -1, don't assert it's value.
     */
    void assertCommand(MemberRc member, int term, int index, String string) throws Exception {
        Command c = member.getCommittedCommand(index, 5000);
        String s = c.getString();
        CoreCmd ccmd = CoreCmd.getCoreCmd(c);
        logger.info("getCommand({}, {}) -> term={}, {}", member.getMemberId(), index, ccmd.term, s);
        assertEquals(s, string);
        if (term > 0) {
            assertEquals(ccmd.term, term);
        }
        c.release();
    }

    /************************** connection test cases ***********************/

    @Test
    public void socketInactivity() throws Exception {
        // Init state
        member1.setCandidate();
        member2.setCandidate();
        member3.setCandidate();
        gondolaRc.tick(200000);
        runningTick = 50;
    }

    
    /************************** log test cases ***********************/

    /**
     * Check that persisting the max gap works.
     */
    @Test
    public void saveMaxGap() throws Exception {
        member1.setMaxGap(99);
        assertEquals(member1.getMaxGap(), 99);
    }
    
    /**
     * The log has a gap. The leader should discard the command after the gap and then insert a no-op.
     */
    @Test
    public void missingEntry() throws Exception {
        // Init state
        int term = 1;
        member1.insert(term, 1, "command 1");
        member1.insert(term, 3, "command 3");
        member1.saveVote(term, -1);
        member1.setMaxGap(1);
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setLeader();
        runningTick = 50;

        // Insert a new command and ensure the command 3 is gone
        commit(member1, "command 2");
        assertCommand(member3, -1, 1, "command 1");
        assertCommand(member3, -1, 2, "command 2");
    }
    
    /**
     * The log has an entry that has not been committed. It should be overwritten.
     */
    @Test
    public void oldEntry() throws Exception {
        // Init state
        int term = 1;
        member1.insert(term, 1, "command 1");
        member1.insert(term, 3, "command 3");
        member1.saveVote(term, -1);
        member1.setMaxGap(1);
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setLeader();
        runningTick = 50;

        // Insert a new command and ensure the command 3 is gone
        commitAsync(member1, "command 2", 0);
        assertCommand(member3, -1, 1, "command 1");
        assertCommand(member3, -1, 2, "command 2");
    }
    
    /**
     * A new leader writes a no-op if it has uncommitted entries.
     */
    @Test
    public void newLeaderNoop() throws Exception {
        // Init state
        int term = 5;
        int cterm = 10;
        member1.insert(term, 1, "command 1");
        member2.insert(term, 1, "command 1");
        member1.saveVote(cterm, -1);
        member2.saveVote(cterm, -1);
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setCandidate();
        member2.setCandidate();
        member3.setCandidate();

        // Exit when a leader is elected
        long leaderCount = 0;
        while (leaderCount == 0) {
            gondolaRc.tick(50);
            leaderCount = members.stream().filter(m -> m.cmember.isLeader()).count();
        }
        runningTick = 50;

        // Member3 can't be a leader
        assertTrue(!member3.cmember.isLeader());

        // First command
        assertCommand(member1, 5, 1, "command 1");

        // Second entry should be a empty command
        assertCommand(member1, -1, 2, "");
    }
    
    /**
     * The log has two no-ops in a row. 
     */
    @Test
    public void twoNoops() throws Exception {
        // Init state
        int term = 1;
        member1.insert(term, 1, "");
        member1.insert(term + 1, 2, "");
        member1.insert(term + 1, 3, "command 1");
        member1.saveVote(term, -1);
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setLeader();
        runningTick = 50;

        assertCommand(member3, -1, 1, "");
        assertCommand(member3, -1, 2, "");
        assertCommand(member3, -1, 3, "command 1");
    }
    
    /**
     * The log has two no-ops in a row. 
     */
    @Test
    public void twoNoopsWithOldEntries() throws Exception {
        // Init state
        int term = 1;
        member1.insert(term + 1, 1, "");
        member1.insert(term + 2, 2, "");
        member1.insert(term + 2, 3, "command 1");
        member1.saveVote(term, -1);
        member2.insert(term, 1, "to be deleted");
        member2.insert(term, 2, "to be deleted");
        member2.insert(term, 3, "to be deleted");
        member2.insert(term, 4, "to be deleted");
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setLeader();
        runningTick = 50;

        assertCommand(member3, -1, 1, "");
        assertCommand(member3, -1, 2, "");
        assertCommand(member3, -1, 3, "command 1");
    }
    
    /************************** election test cases ***********************/

    /**
     * All nodes start out as followers.
     */
    @Test
    public void election() throws Exception {
        gondolaRc.tick(200);

        // Exit when a leader has been elected
        long leaderCount = 0;
        while (leaderCount == 0) {
            gondolaRc.tick(10);
            leaderCount = members.stream().filter(m -> m.cmember.isLeader()).count();
        }
        assertTrue(leaderCount == 1, "More than one leader elected");
    }
    
    /**
     * The term after an election is greater than the term before the election.
     */
    @Test
    public void termIncreases() throws Exception {
        // Init state
        int term = 10;
        member1.insert(term, 1, "command 1");
        member2.insert(term, 1, "command 1");
        member3.insert(term, 1, "command 1");
        member1.saveVote(term, -1);
        member2.saveVote(term, -1);
        member3.saveVote(term, -1);
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setCandidate();
        member2.setCandidate();
        member3.setCandidate();

        // Exit when a leader has been elected
        long leaderCount = 0;
        while (leaderCount == 0) {
            gondolaRc.tick(25);
            leaderCount = members.stream().filter(m -> m.cmember.isLeader()).count();
        }
        assertTrue(member1.cmember.currentTerm > term);
    }
    
    /**
     * Two nodes have lower term but newest logs. Other node has higher term but older log.
     */
    @Test
    public void electionHigherTermAndOlderLog() throws Exception {
        // Init state
        int term = 1;
        member1.insert(term, 1, "command 1");
        member2.insert(term, 1, "command 1");
        member1.saveVote(term, -1);
        member2.saveVote(term, -1);
        member3.saveVote(term + 1, -1);
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setCandidate();
        member2.setCandidate();
        member3.setCandidate();

        // Exit when a leader has been elected
        long leaderCount = 0;
        while (leaderCount == 0) {
            gondolaRc.tick(25);
            leaderCount = members.stream().filter(m -> m.cmember.isLeader()).count();
        }
    }
    
    /**
     * A leader (term 101) wrote in it's log but did not succeed in sending it out. This entry should be deleted.
     */
    @Test
    public void electionShortTermLeader() throws Exception {
        // Init state
        member1.insert(101, 1, "command 2");
        member2.insert(100, 1, "command 1");
        member3.insert(100, 1, "command 1");
        member2.insert(102, 2, "command 3");
        member3.insert(102, 2, "command 3");
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setFollower();
        member2.setFollower();
        member3.setLeader();
        runningTick = 50;

        assertCommand(member1, 100, 1, "command 1");
    }
    
    /**
     * Only the members with the longer log can be a leader.
     */
    @Test
    public void electionWithLongerLog() throws Exception {
        // Init state
        int term = 1;
        member1.insert(term, 1, "command 1");
        member2.insert(term, 1, "command 1");
        member1.saveVote(term, -1);
        gondolaRc.resetMembers(); // Pick up new storage state

        // Exit when a leader is elected
        long leaderCount = 0;
        gondolaRc.tick(100);
        while (leaderCount == 0) {
            gondolaRc.tick(50);
            leaderCount = members.stream().filter(m -> m.cmember.isLeader()).count();
        }

        // Member3 can't be a leader
        assertTrue(!member3.cmember.isLeader());
    }
    
    /**
     * Only the members with the later log can be a leader.
     */
    @Test
    public void electionWithHigherLogTerm() throws Exception {
        // Init state
        member1.insert(1, 1, "command 1");
        member2.insert(1, 1, "command 1");
        member3.insert(1, 1, "command 1");
        member1.insert(2, 2, "command 2");
        member2.insert(2, 2, "command 2");
        gondolaRc.resetMembers(); // Pick up new storage state
        gondolaRc.tick(100);

        // Exit when a leader is elected
        long leaderCount = 0;
        while (leaderCount == 0) {
            gondolaRc.tick(50);
            leaderCount = members.stream().filter(m -> m.cmember.isLeader()).count();
        }

        // Member3 can't be a leader
        assertTrue(!member3.cmember.isLeader());
    }

    /**
     * Only the members with the later log can be a leader.
     */
    @Test
    public void electionWithHigherLogIndex() throws Exception {
        // Init state
        member1.insert(1, 1, "command 1");
        member2.insert(1, 1, "command 1");
        member3.insert(1, 1, "command 1");
        member1.insert(1, 2, "command 2");
        member2.insert(1, 2, "command 2");
        gondolaRc.resetMembers(); // Pick up new storage state
        gondolaRc.tick(100);

        // Exit when a leader is elected
        long leaderCount = 0;
        while (leaderCount == 0) {
            gondolaRc.tick(50);
            leaderCount = members.stream().filter(m -> m.cmember.isLeader()).count();
        }

        // Member3 can't be a leader
        assertTrue(!member3.cmember.isLeader());
    }

    /**
     * Force a leader to become a candidate and induce a re-election.
     * Repeat twice
     */
    @Test
    public void reelection() throws Exception {
        gondolaRc.tick(200);

        int count = 0;
        while (count < 2) {
            gondolaRc.tick(10);

            // Force the leader to become a candidate
            for (MemberRc m : members) {
                if (m.cmember.isLeader()) {
                    m.setCandidate();
                    count++;
                }
            }
        }
    }

    /**
     * The candidates should follow the leader.
     */
    @Test
    public void leaderWithCandidates() throws Exception {
        // Init state
        member1.setLeader();
        member2.setCandidate();
        member3.setCandidate();

        gondolaRc.tick(100);
    }

    /**
     * If two leaders with identical state, one leader should become follower.
     */
    @Test
    public void twoLeaders() throws Exception {
        // Init state
        member1.setLeader();
        member2.setLeader();
        member3.setCandidate();

        // Exit when only one leader survives
        long leaderCount = 2;
        while (true) {
            gondolaRc.tick(10);
            leaderCount = members.stream().filter(m -> m.cmember.isLeader()).count();
            if (leaderCount == 1) {
                break;
            }
        }
    }

    /**
     * Leader/candidate becomes follower if higher term encountered.
     */
    @Test
    public void followIfHigherTerm() throws Exception {
        // Init state
        member1.saveVote(1, -1);
        member2.saveVote(10, -1);
        member3.saveVote(1, -1);
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setLeader();
        member2.setCandidate();
        member3.setCandidate();

        // Exit when there are no leaders
        long count = 1;
        while (count > 0 && !member2.isLeader()) {
            gondolaRc.tick(50);
            count = members.stream().filter(m -> m.cmember.isLeader()).count();
        }
    }

    /**
     * Leader steps down if no followers.
     */
    @Test
    public void leaderStepsDown() throws Exception {
        // Init state
        member1.setLeader();
        member2.setFollower();
        member3.setFollower();
        member1.pauseDelivery(true); // member1 stops getting messages

        // Exit when member1 is no longer a leader
        long count = 1;
        while (member1.cmember.isLeader()) {
            gondolaRc.tick(50);
            count = members.stream().filter(m -> m.cmember.isLeader()).count();
        }
    }

    /**
     * Leader elected with one node down.
     */
    //@Test
    public void leaderWithTwoNodes() throws Exception {
        // Init state
        member1.setFollower();
        member2.setFollower();
        member3.setFollower();
        member1.pauseDelivery(true);

        // Exit when a leader is found
        long count = 0;
        while (count == 0) {
            gondolaRc.tick(50);
            count = members.stream().filter(m -> m.cmember.isLeader()).count();
        }
    }

    /**
     * A candidate should not accept a real vote unless it had a majority of prevotes.
     */
    //@Test
    public void prevotesOnly() throws Exception {
        int term = 5;
        member1.saveVote(term, 1);
        member2.saveVote(term, -1);
        member3.saveVote(term, -1);
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setCandidate();
        member2.setFollower();
        member3.setFollower();
        member1.deliverRequestVoteReply(member2, term, false, true);
        gondolaRc.tick(100);

        // eember1 can't be a leader
        assertTrue(!member1.cmember.isLeader());
    }
    
    /**
     * All nodes start out as followers then change to candidate. Ensure that an event is fired.
     */
    @Test
    public void roleChange() throws Exception {
        member1.insert(1, 1, "command 1");
        gondolaRc.resetMembers(); // Pick up new storage state
        Consumer<RoleChangeEvent> listener = crevt -> {
            if (crevt.newRole == Role.CANDIDATE) {
                assertNull(crevt.leader);
                assertEquals(crevt.oldRole, Role.FOLLOWER);
            }
            if (crevt.newRole == Role.LEADER) {
                assertEquals(crevt.leader.getMemberId(), 1);
                assertEquals(crevt.oldRole, Role.CANDIDATE);
            }
            assertEquals(crevt.member.getMemberId(), member1.getMemberId());
            Assert.assertNotNull(crevt.member.getAddress());
        };
        member1.registerForRoleChanges(listener);

        // Exit when a leader has been elected
        long leaderCount = 0;
        while (leaderCount == 0) {
            gondolaRc.tick(25);
            leaderCount = members.stream().filter(m -> m.cmember.isLeader()).count();
        }
        member1.unregisterForRoleChanges(listener);
    }
    
    /************************** backfill test cases ***********************/

    /**
     * Fill one member with 1000 old values that need to be completely deleted and replaced 1000 new values.
     */
    @Test
    public void largeBackfill() throws Exception {
        // Init state
        for (int i=1; i<=1000; i++) {
            member1.insert(1, i, "older "+i);
        }
        for (int i=1; i<=1000; i++) {
            member2.insert(2, i, "newer "+i);
            member3.insert(2, i, "newer "+i);
        }
        gondolaRc.resetMembers(); // Pick up new storage state
        runningTick = 50;

        // Retrieve the command after member 1 is backfilled
        Command c = member1.getCommittedCommand(1000); // avoid timeout in assertCommand
        assertCommand(member1, 2, 1000, "newer 1000");
    }

    /**
     * The log has 10 records, each record has a different term.
     * Backfill a node given this log state.
     */
    @Test
    public void backfillDiffTerms() throws Exception {
        // Init state
        int cterm = 100;
        int term = 5;
        for (int i=1; i<=10; i++) {
            member1.insert(term+i, i, "command "+i);
            member2.insert(term+i, i, "command "+i);
        }
        member1.saveVote(cterm, -1);
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setLeader();
        member2.setFollower();
        member3.setFollower();
        runningTick = 50;

        // Retrieve the command after member 3 is backfilled
        for (int i=1; i<=10; i++) {
            assertCommand(member3, term+i, i, "command "+i);
        }
    }

    /**
     * The log has 10 records, each record has the same term.
     * Backfill a node given this log state.
     */
    @Test
    public void backfillSameTerms() throws Exception {
        // Init state
        int cterm = 100;
        int term = 5;
        for (int i=1; i<=10; i++) {
            member1.insert(term, i, "command "+i);
            member2.insert(term, i, "command "+i);
        }
        member1.saveVote(cterm, -1);
        gondolaRc.resetMembers(); // Pick up new storage state

        member1.setLeader();
        member2.setFollower();
        member3.setFollower();

        runningTick = 50;

        // Retrieve the command after member 3 is backfilled
        for (int i=1; i<=10; i++) {
            assertCommand(member3, term, i, "command "+i);
        }
    }

    /**
     * One leader backfilling one record to two followers.
     */
    @Test
    public void backfill() throws Exception {
        // Init state
        int term = 1;
        member1.insert(term, 1, "command 1");
        member1.saveVote(term, -1);
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setLeader();
        gondolaRc.tick(100);
        runningTick = 50;

        // Retrieve the command after member 3 is backfilled
        assertCommand(member2, term, 1, "command 1");
    }

    /**
     * Ensure that the matchIndex eventually equals the local savedIndex.
     */
    @Test
    public void backfillUntilUpToDate() throws Exception {
        // Init state
        int term = 5;
        for (int i=1; i<=10; i++) {
            member1.insert(term+i, i, "command "+i);
        }
        gondolaRc.resetMembers(); // Pick up new storage state
        member1.setLeader();
        member2.setFollower();
        member3.setFollower();

        Member m2 = member1.getCluster().getMember(member2.getMemberId());
        while (!m2.isLogUpToDate()) {
            gondolaRc.tick(50);
        }
        assertEquals(member1.getCluster().getLastSavedIndex(), 10);
    }

    /************************** command test cases ***********************/

    /**
     * Commands start at index 1. Fail when getting index 0.
     */
    @Test
    public void commandIndex0() throws Exception {
        try {
            assertCommand(member3, 1, 0, "");
            Assert.fail();
        } catch (Exception e) {
            assertTrue(true);
        }
    }

    /**
     * Writing to a non-leader results in an exception.
     */
    @Test
    public void commandNonLeader() throws Exception {
        try {
            commit(member1, "");
            Assert.fail();
        } catch (Exception e) {
            assertTrue(true);
        }
    }

    /**
     * Storage.hasLogEntry for non-existent log entries should return false.
     */
    @Test
    public void hasIndex0() throws Exception {
        assertTrue(!member1.getGondola().getStorage().hasLogEntry(member1.getMemberId(), 0, 1));

        assertNull(member1.getGondola().getStorage().getLogEntry(member1.getMemberId(), 1));
    }

    /**
     * Commit a command and read it back.
     */
    @Test
    public void getCommand1() throws Exception {
        member1.setLeader();
        runningTick = 50;

        commit(member1, "command 1");

        // Retrieve the command from each member
        assertCommand(member1, -1, 1, "command 1");
        assertCommand(member2, -1, 1, "command 1");
        assertCommand(member3, -1, 1, "command 1");
    }

    /**
     * Commit two commands and read them back.
     */
    @Test
    public void getCommand2() throws Exception {
        member1.setLeader();

        commit(member1, "command 1");
        commit(member1, "command 2");

        // Retrieve the 1st and 2nd command
        assertCommand(member1, -1, 1, "command 1");
        assertCommand(member1, -1, 2, "command 2");

        // Retrieve the 1st command again
        assertCommand(member1, -1, 1, "command 1");
    }

    /**
     * Wait for a command before it's committed.
     */
    @Test
    public void waitCommand() throws Exception {
        member1.setLeader();
        runningTick = 50;

        commitAsync(member1, "command 1", 300);

        // Retrieve the command from each member
        assertCommand(member1, -1, 1, "command 1");
        assertCommand(member2, -1, 1, "command 1");
        assertCommand(member3, -1, 1, "command 1");
    }

    /**
     * Test timeout exception while waiting for a committed command.
     */
    @Test
    public void getCommandTimeout() throws Exception {
        runningTick = 5;

        // Timeout in 1 ms
        try {
            Command c = member1.getCommittedCommand(100, 1);
            Assert.fail();
        } catch (TimeoutException e) {
            assertTrue(true);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    /**
     * When a commit timeouts, status should be non-zero.
     */
    @Test
    public void commitTimeout() throws Exception {
        // Init state
        member1.setLeader();
        member2.setFollower();
        member3.setFollower();
        member1.pauseDelivery(true); // prevent advance of commit index
        runningTick = 50;

        // Commit should timeout in 1 ms
        Command command = member1.checkoutCommand();
        try {
            command.commit(new byte[0], 0, 0, 1);
            Assert.fail();
        } catch (TimeoutException e) {
            assertTrue(true);
        } catch (Exception e) {
            Assert.fail();
        }

        // Status should be timeout
        Assert.assertEquals(command.getStatus(), Command.STATUS_TIMEOUT);
        command.release();
    }

    /************************* utilities **********************/

    class SummaryThread extends Thread {
        public SummaryThread() {
            setName("GondolaTestSummary");
            setDaemon(true);
        }

        public void run() {
            while (true) {
                long now = System.currentTimeMillis();
                try {
                    if (runningTick > 0) {
                        gondolaRc.tick(runningTick);
                    }
                    if (now > startTimer + 5000) {
                        logger.info("Executing " + currentTest);
                        gondolaRc.showSummaries();

                        // No need to print another for a while
                        startTimer = now + 60 * 1000;
                    }
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
