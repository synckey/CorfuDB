package org.corfudb.logreplication.receive;

import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.logreplication.message.LogReplicationEntry;
import org.corfudb.logreplication.message.LogReplicationEntryMetadata;
import org.corfudb.logreplication.message.MessageType;
import org.corfudb.logreplication.fsm.LogReplicationConfig;

import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@NotThreadSafe
@Slf4j
/**
 * Process TxMessage that contains transaction logs for registered streams.
 */
public class LogEntryWriter {
    private Set<UUID> streamUUIDs; //the set of streams that log entry writer will work on.
    HashMap<UUID, IStreamView> streamViewMap; //map the stream uuid to the streamview.
    CorfuRuntime rt;
    private long srcGlobalSnapshot; //the source snapshot that the transaction logs are based
    private long lastMsgTs; //the timestamp of the last message processed.
    private HashMap<Long, LogReplicationEntry> msgQ; //If the received messages are out of order, buffer them. Can be queried according to the preTs.
    private final int MAX_MSG_QUE_SIZE = 20; //The max size of the msgQ.

    public LogEntryWriter(CorfuRuntime rt, LogReplicationConfig config) {
        this.rt = rt;
        Set<String> streams = config.getStreamsToReplicate();
        streamUUIDs = new HashSet<>();

        for (String s : streams) {
            streamUUIDs.add(CorfuRuntime.getStreamID(s));
        }
        msgQ = new HashMap<>();
        srcGlobalSnapshot = Address.NON_ADDRESS;
        lastMsgTs = Address.NON_ADDRESS;

        streamViewMap = new HashMap<>();

        for (UUID uuid : streamUUIDs) {
            streamViewMap.put(uuid, rt.getStreamsView().getUnsafe(uuid));
        }
    }

    /**
     * Verify the metadata is the correct data type.
     * @param metadata
     * @throws ReplicationWriterException
     */
    void verifyMetadata(LogReplicationEntryMetadata metadata) throws ReplicationWriterException {
        if (metadata.getMessageMetadataType() != MessageType.LOG_ENTRY_MESSAGE) {
            log.error("Wrong message metadata {}, expecting  type {} snapshot {}", metadata,
                    MessageType.LOG_ENTRY_MESSAGE, srcGlobalSnapshot);
            throw new ReplicationWriterException("wrong type of message");
        }
    }

    /**
     * Convert message data to an MultiObjectSMREntry and write to log.
     * @param txMessage
     */
    void processMsg(LogReplicationEntry txMessage) {
        // Convert from byte[] to OpaqueEntry
        OpaqueEntry opaqueEntry = OpaqueEntry.deserialize(Unpooled.wrappedBuffer(txMessage.getPayload()));
        Map<UUID, List<SMREntry>> map = opaqueEntry.getEntries();

        if (!streamUUIDs.containsAll(map.keySet())) {
            log.error("txMessage contains noisy streams {}, expecting {}", map.keySet(), streamUUIDs);
            throw new ReplicationWriterException("Wrong streams set");
        }

        try {
            MultiObjectSMREntry multiObjectSMREntry = new MultiObjectSMREntry();

            for (UUID uuid : opaqueEntry.getEntries().keySet()) {
                for(SMREntry smrEntry : opaqueEntry.getEntries().get(uuid)) {
                    multiObjectSMREntry.addTo(uuid, smrEntry);
                }
            }
            rt.getStreamsView().append(multiObjectSMREntry, null, opaqueEntry.getEntries().keySet().toArray(new UUID[0]));
        } catch (Exception e) {
            log.warn("Caught an exception ", e);
            throw e;
        }

        lastMsgTs = txMessage.getMetadata().getTimestamp();
    }

    /**
     * Go over the queue, if the expecting messages in queue, process it.
     */
    void processQueue() {
        while (true) {
            LogReplicationEntry txMessage = msgQ.get(lastMsgTs);
            if (txMessage == null) {
                return;
            }
            processMsg(txMessage);
            msgQ.remove(lastMsgTs);
        }
    }

    /**
     * Remove entries that has timestamp smaller than msgTs
     * @param msgTs
     */
    void cleanMsgQ(long msgTs) {
        for (long address : msgQ.keySet()) {
            if (msgQ.get(address).getMetadata().getTimestamp() <= lastMsgTs) {
                msgQ.remove(address);
            }
        }
    }

    /**
     * Apply message generate by log entry reader and will apply at the destination corfu cluster.
     * @param msg
     * @return long: the last processed message timestamp if apply processing any messages.
     * @throws ReplicationWriterException
     */
    public long apply(LogReplicationEntry msg) throws ReplicationWriterException {

        verifyMetadata(msg.getMetadata());

        // Ignore the out of date messages
        if (msg.getMetadata().getSnapshotTimestamp() < srcGlobalSnapshot) {
            log.warn("Received message with snapshot {} is smaller than current snapshot {}.Ignore it",
                    msg.getMetadata().getSnapshotTimestamp(), srcGlobalSnapshot);
            return Address.NON_ADDRESS;
        }

        // A new Delta sync is triggered, setup the new srcGlobalSnapshot and msgQ
        if (msg.getMetadata().getSnapshotTimestamp() > srcGlobalSnapshot) {
            srcGlobalSnapshot = msg.getMetadata().getSnapshotTimestamp();
            lastMsgTs = srcGlobalSnapshot;
            cleanMsgQ(lastMsgTs);
        }

        // we will skip the entries has been processed.
        if (msg.getMetadata().getTimestamp() <= lastMsgTs) {
            return Address.NON_ADDRESS;
        }

        //If the entry is the expecting entry, process it and process
        //the messages in the queue.
        if (msg.getMetadata().getPreviousTimestamp() == lastMsgTs) {
            processMsg(msg);
            processQueue();
            return lastMsgTs;
        }

        //If the entry's ts is larger than the entry processed, put it to the queue
        if (msgQ.size() < MAX_MSG_QUE_SIZE) {
            msgQ.putIfAbsent(msg.getMetadata().getPreviousTimestamp(), msg);
        } else if (msgQ.get(msg.getMetadata().getPreviousTimestamp()) != null) {
            log.warn("The message is out of order and the queue is full, will drop the message {}", msg.getMetadata());
        }

        return Address.NON_ADDRESS;
    }

    /**
     * Set the basesnapshot that last full sync based on and ackTimesstamp
     * that is the last log entry it has played.
     * This is called while the writer enter the log entry sync state.
     * @param snapshot
     * @param ackTimestamp
     */
    public void setTimestamp(long snapshot, long ackTimestamp) {
        srcGlobalSnapshot = snapshot;
        lastMsgTs = ackTimestamp;
        cleanMsgQ(ackTimestamp);
    }
}