/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola;

import com.yahoo.gondola.core.CoreMember;
import com.yahoo.gondola.core.Peer;
import com.yahoo.gondola.core.Stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * The type Shard.
 */
public class Shard implements Stoppable {

    final static Logger logger = LoggerFactory.getLogger(Shard.class);

    final Gondola gondola;
    final Config config;
    final Stats stats;
    final String shardId;
    final List<Peer> peers = new ArrayList<>();
    final List<Member> members = new ArrayList<>();
    Member localMember;
    CoreMember cmember;

    Shard(Gondola gondola, String shardId) throws Exception {
        this.gondola = gondola;
        this.shardId = shardId;

        config = gondola.getConfig();
        stats = gondola.getStats();
        List<Config.ConfigMember> configMembers = config.getMembersInShard(shardId);

        List<Integer> peerIds = configMembers.stream()
            .filter(cm -> !cm.hostId.equals(gondola.getHostId()))
            .map(cm -> cm.memberId).collect(Collectors.toList());

        // First create the local member, because it's needed when creating the remote members.
        for (int i = 0; i < configMembers.size(); i++) {
            Config.ConfigMember cm = configMembers.get(i);
            if (gondola.getHostId().equals(cm.hostId)) {
                // Local member
                boolean isPrimary = i == 0;
                cmember = new CoreMember(gondola, this, cm.memberId, peerIds, isPrimary);
                localMember = new Member(gondola, cmember, null);
                members.add(localMember);
                break;
            }
        }
        if (cmember == null) {
            throw new IllegalStateException(String.format("Host id %s not found in %d", config.getIdentifier()));
        }

        // Create list of peers
        for (Peer p : cmember.peers) {
            members.add(new Member(gondola, cmember, p));
        }
    }

    public void start() throws Exception {
        cmember.start();
    }

    public boolean stop() {
        return cmember.stop();
    }

    /******************** methods *********************/

    /**
     * Returns the cluster as provided in the constructor. See Cluster().
     *
     * @return non-null cluster id.
     */
    public String getShardId() {
        return shardId;
    }

    /**
     * Returns the member with the specified id.
     *
     * @return null if the member id is not known.
     */
    public Member getMember(int id) {
        for (Member m : members) {
            if (m.getMemberId() == id) {
                return m;
            }
        }
        return null;
    }

    /**
     * @return non-null list of members in this cluster.
     */
    public List<Member> getMembers() {
        return members;
    }

    /**
     * Returns the local member of this cluster. The local member is the actual process running on this host. The remote
     * members are proxies of members running on other hosts.
     *
     * @return non-null local member.
     */
    public Member getLocalMember() {
        return localMember;
    }

    /**
     * Returns the list of non-local members.
     *
     * @return non-null list of non-local members.
     */
    public List<Member> getRemoteMembers() {
        return members.stream().filter(m -> !m.isLocal()).collect(Collectors.toList());
    }

    /**
     * Returns the current leader.
     *
     * @return null if the leader is not known.
     */
    public Member getLeader() {
        return getMember(cmember.getLeaderId());
    }

    /**
     * Forces the local member of this cluster to become the leader. Blocks until this member becomes the leader.
     *
     * @param timeout -1 means there is no timeout.
     */
    public void forceLeader(int timeout) {
        long start = System.currentTimeMillis();
        while (!cmember.isLeader()) {
            try {
                cmember.forceLeader();
                if (timeout >= 0 && System.currentTimeMillis() - start > timeout) {
                    break;
                }
                Thread.sleep(1000);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public Role getLocalRole() {
        return cmember.getRole();
    }

    /**
     * Returns the last saved index for this cluster.
     */
    public int getLastSavedIndex() throws Exception {
        return cmember.getSavedIndex();
    }

    /**
     * ******************* commands ******************
     */

    // TODO: move pool to Gondola, like message pool
    Queue<Command> pool = new ConcurrentLinkedQueue<>();

    /**
     * Retrieves a command object from the command pool. Blocks until there are commands in the pool.
     *
     * @return non-null command object
     */
    public Command checkoutCommand() throws InterruptedException {
        Command command = pool.poll();
        if (command == null) {
            command = new Command(gondola, this, cmember);
        }
        return command;
    }

    /**
     * Returns the command back into the command pool.
     *
     * @param command non-null command that is no longer used.
     */
    void checkinCommand(Command command) {
        pool.add(command);
    }

    /**
     * Equivalent to getCommittedCommand(index, -1);
     *
     * @param index must be > 0.
     * @return the non-null Command at index.
     */
    public Command getCommittedCommand(int index) throws Exception {
        return getCommittedCommand(index, -1);
    }

    /**
     * Returns the command at the specified index. This method blocks until index has been committed. An empty command
     * can be returned. This is an artifact of the raft protocol to avoid deadlock when used with a finite thread pool.
     * Empty commands will be inserted right after a leader election when the new leader discovers that it has
     * uncommitted commands. The leader inserts an empty command to commit these immediately.
     *
     * @param index   Must be > 0.
     * @param timeout Returns after timeout milliseconds, even if the command is not yet available. -1 means there is no
     *                timeout.
     * @return non-null Command
     */
    public Command getCommittedCommand(int index, int timeout) throws Exception {
        if (index <= 0) {
            throw new IllegalStateException(String.format("Index %d must be > 0", index));
        }
        Command command = checkoutCommand();
        try {
            cmember.getCommittedLogEntry(command.ccmd, index, timeout);
            return command;
        } catch (Exception e) {
            command.release();
            throw e;
        }
    }

    /**
     * Returns commitIndex.
     */
    public int getCommitIndex() {
        return cmember.getCommitIndex();
    }
}
