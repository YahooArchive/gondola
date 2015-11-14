/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.core;

import com.yahoo.gondola.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a Raft message in byte array form.
 * Behaves like a struct on a byte array.
 * A message is reference counted. When the reference count reaches 0,
 * the message is automatically returned to the pool.
 * <p>
 * Usage:
 * Message message = pool.checkout();
 * message.handle(messageHandler);
 * message.release();
 */
public class Message {
    final static Logger logger = LoggerFactory.getLogger(Message.class);
    MessagePool pool;
    Stats stats;

    // Used to help track message leaks
    static AtomicInteger idGenerator = new AtomicInteger();
    int id;

    // The number of bytes in this message
    public int size; // public for tests

    // The size of the first command in this message
    int commandSize;

    // The number of commands batched in this message
    int numCommands;

    // Holds the bytes that are to be sent or have been received.
    public byte[] buffer; // public for tests

    // Position of the command in buffer. Only relevant for append entry requests
    int bufferOffset;

    // Overlay on buffer to encode ints, etc.
    ByteBuffer byteBuffer;

    // If message tracing is enabled, this string will be set with tracing information that can be displayed by the caller
    public String tracingInfo; // public for tests

    // Freshly checked-out messages have a reference count of 1.
    // When the reference count reaches 0, it is returned to the pool.
    AtomicInteger refCount = new AtomicInteger();

    public final static int TYPE_APPEND_ENTRY_REQ = 0;
    public final static int TYPE_APPEND_ENTRY_REP = 1;
    public final static int TYPE_REQUEST_VOTE_REQ = 2;
    public final static int TYPE_REQUEST_VOTE_REP = 3;
    final static int NUM_TYPES = 4;

    // Contains the overhead for each message type. The overhead contains the header and non-optional fields.
    final static int[] overhead = new int[NUM_TYPES];
    final static int HEADER_OVERHEAD = 1 * 4 + 2 * 2 + 1;
    static int maxOverhead = 0;

    // Layout of header
    final static int HEADER_OFFSET_SIZE = 0;
    final static int HEADER_OFFSET_TYPE = 2;
    final static int HEADER_OFFSET_FROM = 3;
    final static int HEADER_OFFSET_TERM = 5;
    final static int HEADER_OFFSET = 9;

    // Message variables
    int type;
    int term;
    int fromMemberId;
    int toMemberId;
    boolean success;
    int mnIndex;
    int commitIndex;
    boolean isHeartbeat;
    int entryTerm;
    boolean isPrevote;
    boolean voteGranted;
    long timestamp;
    public Rid prevRid = new Rid(); // public because needed for test

    // Config
    static boolean messageTracing;
    static boolean batching;
    static volatile int maxCommandSize = -1;
    static boolean heartbeatTracing;

    // Overhead constants
    static {
        // These numbers don't yet include the header overhead. Will be added in the next step.
        overhead[TYPE_APPEND_ENTRY_REQ] = 4 * 4 + 2 * 1;
        overhead[TYPE_APPEND_ENTRY_REP] = 1 * 4 + 2 * 1;
        overhead[TYPE_REQUEST_VOTE_REQ] = 2 * 4 + 1;
        overhead[TYPE_REQUEST_VOTE_REP] = 0 * 4 + 2 * 1;

        for (int i = 0; i < overhead.length; i++) {
            // The overhead includes the first 9 bytes that contain the message length, type, and targe member id
            overhead[i] += HEADER_OVERHEAD;
            maxOverhead = Math.max(maxOverhead, overhead[i]);
        }
    }

    public Message(Config config, MessagePool pool, Stats stats) {
        this.pool = pool;
        this.stats = stats;
        this.id = idGenerator.incrementAndGet();

        buffer = new byte[maxCommandSize + maxOverhead];
        byteBuffer = ByteBuffer.wrap(buffer);
    }

    /**
     * Must be called before message objects can be created.
     */
    public static void initConfig(Config config) {
        config.registerForUpdates(config1 -> {
                maxCommandSize = config1.getInt("raft.command_max_size");
                messageTracing = config1.getBoolean("tracing.raft_messages");
                heartbeatTracing = config1.getBoolean("tracing.raft_heartbeats");
                batching = config1.getBoolean("gondola.batching");
        });
    }

    public boolean isHeartbeat() {
        return type == TYPE_APPEND_ENTRY_REQ && isHeartbeat;
    }

    /**
     * Returns the number of commands in the messsage. A heartbeat contains no commands.
     *
     * @return 0 or number of commands in the message.
     */
    public int numCommands() {
        return numCommands;
    }

    /**
     * Returns true if this message can accomodate another command of cmdSize.
     */
    public boolean canBatch(int cmdSize) {
        assert getType() == TYPE_APPEND_ENTRY_REQ;
        return batching && size + cmdSize + 2 <= buffer.length && !isHeartbeat();
    }

    /**
     * *************************** header *****************************
     */

    public int size() {
        return byteBuffer.getShort(0);
    }

    public int getType() {
        return byteBuffer.get(2);
    }

    public int getFromMemberId() {
        return byteBuffer.getShort(3);
    }

    public int getTerm() {
        return byteBuffer.get(5);
    }

    /****************************** ref counts ******************************/

    /**
     * Increment the reference count. It should be incremented before adding to a queue.
     */
    public void acquire() {
        int n = refCount.incrementAndGet();
    }

    /**
     * Decrement the reference count. When the reference count reaches 0, it is returned to the pool.
     */
    public void release() {
        int n = refCount.decrementAndGet();
        if (n == 0) {
            tracingInfo = null;
            pool.checkin(this);
        }
        assert n >= 0 : String.format("Message is over-released: type=%d from=%d",
                getType(), getFromMemberId());
    }

    /****************************** format message ******************************/

    /**
     * @param memberId the id of the member sending the message.
     */
    void putHeader(int type, int memberId, int term, int commandSize) {
        this.type = type;
        this.fromMemberId = memberId;
        this.term = term;
        this.commandSize = commandSize;
        size = overhead[type] + commandSize;
        byteBuffer.clear();
        byteBuffer.limit(buffer.length);
        byteBuffer.putShort((short) size);
        byteBuffer.put((byte) type);
        byteBuffer.putShort((short) memberId);
        byteBuffer.putInt(term);

        assert byteBuffer.position() == HEADER_OVERHEAD;
    }

    public void heartbeat(int memberId, int term, Rid prevRid, int commitIndex) {
        putHeader(TYPE_APPEND_ENTRY_REQ, memberId, term, 0);
        byteBuffer.putInt(prevRid.term);
        byteBuffer.putInt(prevRid.index);
        byteBuffer.putInt(commitIndex);
        byteBuffer.putInt(0); // entryTerm
        byteBuffer.putShort((short) 0); // command size

        stats.appendEntryRequest();
        if (messageTracing && heartbeatTracing) {
            tracingInfo = String.format("H(cterm=%d log=(%d,%d) ci=%d)",
                    term, prevRid.term, prevRid.index, commitIndex);
        }
        this.prevRid.set(prevRid);
        this.commitIndex = commitIndex;
        this.isHeartbeat = true;
        this.entryTerm = 0;
        this.numCommands = 0;
    }

    public void appendEntryRequest(int memberId, int term, Rid prevRid, int commitIndex,
                                   int entryTerm, byte[] buf, int offset, int len) {
        putHeader(TYPE_APPEND_ENTRY_REQ, memberId, term, len);
        byteBuffer.putInt(prevRid.term);
        byteBuffer.putInt(prevRid.index);
        byteBuffer.putInt(commitIndex);
        byteBuffer.putInt(entryTerm);
        byteBuffer.putShort((short) len);
        bufferOffset = byteBuffer.position();
        byteBuffer.put(buf, offset, len);
        assert byteBuffer.position() == size;

        stats.appendEntryRequest();
        if (messageTracing) {
            tracingInfo = String.format("AE(cterm=%d pterm=%d eterm=%d index=%d ci=%d size=%s)",
                    term, prevRid.term, entryTerm, prevRid.index + 1, commitIndex, len);
        }
        this.prevRid.set(prevRid);
        this.commitIndex = commitIndex;
        this.isHeartbeat = false;
        this.entryTerm = entryTerm;
        this.numCommands = 1;
    }

    /**
     * Append another command to this message.
     */
    public void appendEntryBatch(byte[] buf, int offset, int len) {
        if (size + len + 2 > buffer.length) {
            throw new IllegalStateException("There is not enough space for another command");
        }
        assert !isHeartbeat();
        byteBuffer.putShort((short) len);
        byteBuffer.put(buf, offset, len);
        size += len + 2;
        byteBuffer.putShort(0, (short) size);

        stats.appendEntryBatch();
        if (messageTracing) {
            tracingInfo = String.format("AE(cterm=%d pterm=%d index=%d ci=%d size=%d) batch",
                    term, prevRid.term, prevRid.index + 1, commitIndex,
                    size - overhead[TYPE_APPEND_ENTRY_REQ]);
        }
        numCommands++;
    }

    public void appendEntryReply(int memberId, int term, int mnIndex, boolean success, boolean isHeartbeat) {
        putHeader(TYPE_APPEND_ENTRY_REP, memberId, term, 0);
        byteBuffer.putInt(mnIndex);
        byteBuffer.put(success ? (byte) 1 : (byte) 0);
        byteBuffer.put(isHeartbeat ? (byte) 1 : (byte) 0);
        assert byteBuffer.position() == size;

        stats.appendEntryReply();
        if (messageTracing && (heartbeatTracing || !isHeartbeat)) {
            tracingInfo = String.format("%s(cterm=%d %s=%d %s)", isHeartbeat ? "h" : "ae", term,
                    success ? "matchIndex" : "nextIndex", mnIndex, success ? "ok" : "fail");
        }
        this.mnIndex = mnIndex;
        this.success = success;
    }

    public void requestVoteRequest(int memberId, int term, boolean isPrevote, Rid lastRid) {
        putHeader(TYPE_REQUEST_VOTE_REQ, memberId, term, 0);
        byteBuffer.put(isPrevote ? (byte) 1 : (byte) 0);
        byteBuffer.putInt(lastRid.term);
        byteBuffer.putInt(lastRid.index);
        assert byteBuffer.position() == size;

        stats.requestVoteRequest();
        if (messageTracing) {
            tracingInfo = String.format("RV(cterm=%d pterm=%d index=%d) %s",
                    term, lastRid.term, lastRid.index, isPrevote ? "prevote" : "");
        }
        this.prevRid.set(lastRid);
    }

    public void requestVoteReply(int memberId, int term, boolean isPrevote, boolean voteGranted) {
        putHeader(TYPE_REQUEST_VOTE_REP, memberId, term, 0);
        byteBuffer.put(isPrevote ? (byte) 1 : (byte) 0);
        byteBuffer.put(voteGranted ? (byte) 1 : (byte) 0);
        assert byteBuffer.position() == size;

        stats.requestVoteReply();
        if (messageTracing) {
            tracingInfo = String.format("rv(term=%d %s) %s",
                    term, voteGranted ? "yes" : "no", isPrevote ? "prevote" : "");
        }
        this.voteGranted = voteGranted;
    }

    /****************************** handle ******************************/

    /**
     * Delivers this message to the handler.
     * The caller is responsible for releasing this message after calling this method.
     */
    public void handle(MessageHandler handler) throws Exception {
        switch (type) {
            case TYPE_APPEND_ENTRY_REQ:
                // Handle first command
                boolean cont = handler
                        .appendEntryRequest(this, fromMemberId, term, prevRid.term, prevRid.index,
                                commitIndex, isHeartbeat, entryTerm, buffer, bufferOffset, commandSize,
                                size == overhead[TYPE_APPEND_ENTRY_REQ] + commandSize);
                if (!cont) {
                    break;
                }

                // Use temporary variables while iterating on the batch to avoid mutating the message
                int pli = prevRid.index;
                int bo = bufferOffset;
                int cs = commandSize;
                int s = overhead[TYPE_APPEND_ENTRY_REQ] + commandSize;
                int nc = commandSize > 0 ? 1 : 0;
                while (s < size) {
                    bo += cs;
                    cs = (0x0ff & buffer[bo]) << 8 | (0x0ff & buffer[bo + 1]);
                    bo += 2;
                    pli += 1;
                    if (messageTracing) {
                        tracingInfo = String.format("AE(cterm=%d pterm=%d eterm=%d index=%d ci=%d size=%d) batch(%d)",
                                term, prevRid.term, entryTerm, pli + 1, commitIndex, cs, nc);
                    }

                    boolean lastCommand = size == s + cs + 2;
                    cont = handler.appendEntryRequest(this, fromMemberId, term, prevRid.term, pli, commitIndex,
                            isHeartbeat, entryTerm, buffer, bo, cs, lastCommand);
                    if (!cont) {
                        break;
                    }
                    nc++;
                    s += cs + 2;
                }
                break;
            case TYPE_APPEND_ENTRY_REP:
                handler.appendEntryReply(this, fromMemberId, term, mnIndex, success);
                break;
            case TYPE_REQUEST_VOTE_REQ:
                handler.requestVoteRequest(this, fromMemberId, term, isPrevote, prevRid);
                break;
            case TYPE_REQUEST_VOTE_REP:
                handler.requestVoteReply(this, fromMemberId, term, isPrevote, voteGranted);
                break;
        }
    }

    /**
     * Parses the binary message into variables that will be used when handlers are called.
     * The message should be never be mutated after parse() is called.
     */
    void parse() throws Exception {
        ByteBuffer bb = byteBuffer;
        bb.clear();
        size = bb.getShort();
        if (size > buffer.length) {
            throw new IllegalStateException("Message size " + size + " is > buffer size " + buffer.length);
        }
        bb.limit(size);
        type = bb.get();
        fromMemberId = bb.getShort();
        term = bb.getInt();
        commandSize = 0;
        numCommands = 0;
        timestamp = 0;

        switch (type) {
            case TYPE_APPEND_ENTRY_REQ:
                prevRid.set(bb.getInt(), bb.getInt());
                commitIndex = bb.getInt();
                entryTerm = bb.getInt();
                isHeartbeat = entryTerm == 0;
                commandSize = bb.getShort();
                bufferOffset = bb.position();
                if (messageTracing) {
                    if (isHeartbeat) {
                        if (heartbeatTracing) {
                            tracingInfo = String.format("H(cterm=%d eterm=%d index=%d ci=%d)",
                                    term, prevRid.term, prevRid.index, commitIndex);
                        }
                    } else {
                        tracingInfo = String.format("AE(cterm=%d pterm=%d eterm=%d index=%d ci=%d size=%d)",
                                term, prevRid.term, entryTerm, prevRid.index + 1,
                                commitIndex, size - overhead[TYPE_APPEND_ENTRY_REQ]);
                    }
                }

                // Determine number of commands
                numCommands = 1;
                int s = overhead[TYPE_APPEND_ENTRY_REQ] + commandSize;
                int bo = bufferOffset;
                int cs = commandSize;
                while (s < size) {
                    numCommands++;
                    bo += cs;
                    cs = (0x0ff & buffer[bo]) << 8 | (0x0ff & buffer[bo + 1]);
                    bo += 2;
                    s += cs + 2;
                }
                break;
            case TYPE_APPEND_ENTRY_REP:
                mnIndex = bb.getInt();
                success = bb.get() == 1;
                isHeartbeat = bb.get() == 1;
                if (messageTracing && (heartbeatTracing || !isHeartbeat)) {
                    tracingInfo = String.format("%s(cterm=%d %s=%d %s)", isHeartbeat ? "h" : "ae", term,
                            success ? "matchIndex" : "nextIndex", mnIndex, success ? "ok" : "fail");
                }
                break;
            case TYPE_REQUEST_VOTE_REQ:
                isPrevote = bb.get() == 1;
                prevRid.set(bb.getInt(), bb.getInt());
                if (messageTracing) {
                    tracingInfo = String.format("RV(cterm=%d pterm=%d index=%d) %s",
                            term, prevRid.term, prevRid.index, isPrevote ? "prevote" : "");
                }
                break;
            case TYPE_REQUEST_VOTE_REP:
                isPrevote = bb.get() == 1;
                voteGranted = bb.get() == 1;
                if (messageTracing) {
                    tracingInfo = String.format("rv(cterm=%d %s) %s", term, voteGranted ? "yes" : "no", isPrevote ? "prevote" : "");
                }
                break;
            default:
                throw new IllegalStateException("Unknown message type " + type);
        }
    }

    /****************************** read ******************************/

    /**
     * Returns the number of bytes read into the overflow message.
     * If overflow is null, 0 is returned.
     *
     * @param overflow a possibly-null message
     * @return -1 if the input stream returns -1
     */
    public int read(InputStream in, int offset, Message overflow) throws Exception {
        // Safety check to prevent message overwriting
        if (refCount.get() == 0) throw new IllegalStateException("Modifying a message while in the pool");

        // The message size
        int msgSize = offset < 2 ? 2 : byteBuffer.getShort(0);

        // Read at least message size
        while (offset < msgSize) {
            int max = overflow == null ? Math.max(2, msgSize - offset) : buffer.length - offset;
            int n = in.read(buffer, offset, max);
            if (n < 0) {
                return -1;
            }
            offset += n;
            if (offset >= msgSize) {
                // Get the message size
                msgSize = byteBuffer.getShort(0);
            }
        }

        // Copy excess bytes into the overflow message
        int overflowSize = offset - msgSize;
        if (overflowSize > 0) {
            System.arraycopy(buffer, msgSize, overflow.buffer, 0, overflowSize);
        }

        // Parse the binary message into variables, ready to be used by handlers
        parse();
        return overflowSize;
    }

    public void read(byte[] buf, int offset, int len) throws Exception {
        System.arraycopy(buf, offset, buffer, 0, len);
        parse();
    }
}
