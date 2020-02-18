package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.SourceManager;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.fsm.ObservableValue;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.receive.StreamsSnapshotWriter;

import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;

import org.corfudb.logreplication.send.SnapshotReadMessage;
import org.corfudb.logreplication.send.StreamsSnapshotReader;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Start two servers, one as the src, the other as the dst.
 * Copy snapshot data rom src to dst
 */
@Slf4j
public class StreamSnapshotReplicationIT extends AbstractIT implements Observer {

    static final String SOURCE_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    static final int WRITER_PORT = DEFAULT_PORT + 1;
    static final String DESTINATION_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;

    static final UUID REMOTE_SITE_ID = UUID.randomUUID();
    static final String TABLE_PREFIX = "test";

    static private final int NUM_KEYS = 100;
    static private final int NUM_STREAMS = 1;

    Process sourceServer;
    Process destinationServer;

    // Connect with sourceServer to generate data
    CorfuRuntime srcDataRuntime = null;

    // Connect with sourceServer to read snapshot data
    CorfuRuntime readerRuntime = null;

    // Connect with destinationServer to write snapshot data
    CorfuRuntime writerRuntime = null;

    // Connect with destinationServer to verify data
    CorfuRuntime dstDataRuntime = null;

    Random random = new Random();
    HashMap<String, CorfuTable<Long, Long>> srcTables = new HashMap<>();
    HashMap<String, CorfuTable<Long, Long>> dstTables = new HashMap<>();

    CorfuRuntime srcTestRuntime;
    HashMap<String, CorfuTable<Long, Long>> srcTestTables = new HashMap<>();

    CorfuRuntime dstTestRuntime;
    HashMap<String, CorfuTable<Long, Long>> dstTestTables = new HashMap<>();

    /*
     * the in-memory data for corfu tables for verification.
     */
    HashMap<String, HashMap<Long, Long>> srcHashMap = new HashMap<>();
    HashMap<String, HashMap<Long, Long>> dstHashMap = new HashMap<>();

    // An observable value on the number of received ACKs at the sender / source side
    private ObservableValue ackMessages;

    // A semaphore that allows to block until the observed value reaches the expected value
    private final Semaphore blockUntilExpectedValueReached = new Semaphore(1, true);

    // Set per test according to the expected number of acks that will unblock the code waiting for the value change
    private int expectedAckMessages = 0;

    /*
     * store message generated by stream snapshot reader and will play it at the writer side.
     */
    List<DataMessage> msgQ = new ArrayList<>();

    void setupEnv() throws IOException {
        // Source Corfu Server (data will be written to this server)
        sourceServer = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        // Destination Corfu Server (data will be replicated into this server)
        destinationServer = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(WRITER_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        srcDataRuntime = CorfuRuntime.fromParameters(params)
        .setTransactionLogging(true);
        srcDataRuntime.parseConfigurationString(SOURCE_ENDPOINT);
        srcDataRuntime.connect();

        srcTestRuntime = CorfuRuntime.fromParameters(params);
        srcTestRuntime.parseConfigurationString(SOURCE_ENDPOINT);
        srcTestRuntime.connect();

        readerRuntime = CorfuRuntime.fromParameters(params);
        readerRuntime.parseConfigurationString(SOURCE_ENDPOINT);
        readerRuntime.connect();

        writerRuntime = CorfuRuntime.fromParameters(params);
        writerRuntime.parseConfigurationString(DESTINATION_ENDPOINT);
        writerRuntime.connect();

        dstDataRuntime = CorfuRuntime.fromParameters(params);
        dstDataRuntime.parseConfigurationString(DESTINATION_ENDPOINT);
        dstDataRuntime.connect();

        dstTestRuntime = CorfuRuntime.fromParameters(params);
        dstTestRuntime.parseConfigurationString(DESTINATION_ENDPOINT);
        dstTestRuntime.connect();
    }

    void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt, int numStreams) {
        for (int i = 0; i < numStreams; i++) {
            String name = TABLE_PREFIX + i;

            CorfuTable<Long, Long> table = rt.getObjectsView()
                    .build()
                    .setStreamName(name)
                    .setTypeToken(new TypeToken<CorfuTable<Long, Long>>() {
                    })
                    .setSerializer(Serializers.PRIMITIVE)
                    .open();
            tables.put(name, table);
        }
    }

    // Generate data and the same time push the data to the hashtable
    void generateData(HashMap<String, CorfuTable<Long, Long>> tables,
                      HashMap<String, HashMap<Long, Long>> hashMap,
                      int numKeys, CorfuRuntime rt, long startval) {
        for (int i = 0; i < numKeys; i++) {
            for (String name : srcTables.keySet()) {
                hashMap.putIfAbsent(name, new HashMap<>());
                long key = i + startval;
                tables.get(name).put(key, key);
                log.trace("tail " + rt.getAddressSpaceView().getLogTail() + " seq " + rt.getSequencerView().query().getSequence());
                hashMap.get(name).put(key, key);
            }
        }
    }

    // Generate data and the same time push the data to the hashtable
    void generateTXData(HashMap<String, CorfuTable<Long, Long>> tables,
                      HashMap<String, HashMap<Long, Long>> hashMap,
                      int numKeys, CorfuRuntime rt, long startval) {
        for (int i = 0; i < numKeys; i++) {
            for (String name : srcTables.keySet()) {
                rt.getObjectsView().TXBegin();
                hashMap.putIfAbsent(name, new HashMap<>());
                long key = i + startval;
                tables.get(name).put(key, key);
                log.trace("tail " + rt.getAddressSpaceView().getLogTail() + " seq " + rt.getSequencerView().query().getSequence());
                hashMap.get(name).put(key, key);
                rt.getObjectsView().TXEnd();
            }
        }
    }


    // Generate data and the same time push the data to the hashtable
    void generateTransactionsCrossTables(HashMap<String, CorfuTable<Long, Long>> tables,
                                         Set<String> tablesCrossTxs,
                                         HashMap<String, HashMap<Long, Long>> hashMap,
                                         int numKeys, CorfuRuntime rt, long startval) {
        for (int i = 0; i < numKeys; i++) {
            rt.getObjectsView().TXBegin();
            for (String name : tablesCrossTxs) {
                hashMap.putIfAbsent(name, new HashMap<>());
                long key = i + startval;
                tables.get(name).put(key, key);
                hashMap.get(name).put(key, key);
            }
            rt.getObjectsView().TXEnd();
        }
    }

    void verifyData(HashMap<String, CorfuTable<Long, Long>> tables, HashMap<String, HashMap<Long, Long>> hashMap) {
        for (String name : hashMap.keySet()) {
            CorfuTable<Long, Long> table = tables.get(name);
            HashMap<Long, Long> mapKeys = hashMap.get(name);

            System.out.println("table " + name + " key size " + table.keySet().size() +
                    " hashMap size " + mapKeys.size());

            assertThat(mapKeys.keySet().containsAll(table.keySet())).isTrue();
            assertThat(table.keySet().containsAll(mapKeys.keySet())).isTrue();
            assertThat(table.keySet().size() == mapKeys.keySet().size()).isTrue();

            for (Long key : mapKeys.keySet()) {
                assertThat(table.get(key)).isEqualTo(mapKeys.get(key));
            }
        }
    }

    void verifyNoData(HashMap<String, CorfuTable<Long, Long>> tables) {
        for (CorfuTable table : tables.values()) {
            assertThat(table.keySet().isEmpty());
        }
    }

    /**
     * enforce checkpoint entries at the streams.
     */
    void ckStreams(CorfuRuntime rt, HashMap<String, CorfuTable<Long, Long>> tables) {
        MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
        for (CorfuTable map : tables.values()) {
            mcw1.addMap(map);
        }

        Token checkpointAddress = mcw1.appendCheckpoints(rt, "test");

        // Trim the log
        rt.getAddressSpaceView().prefixTrim(checkpointAddress);
        rt.getAddressSpaceView().gc();
    }

    void readMsgs(List<DataMessage> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());
        StreamsSnapshotReader reader = new StreamsSnapshotReader(rt, config);

        reader.reset(rt.getAddressSpaceView().getLogTail());
        while (true) {
            SnapshotReadMessage snapshotReadMessage = reader.read();
            msgQ.addAll(snapshotReadMessage.getMessages());
            if (snapshotReadMessage.isEndRead()) {
                break;
            }
        }
    }

    void writeMsgs(List<DataMessage> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());
        StreamsSnapshotWriter writer = new StreamsSnapshotWriter(rt, config);

        if (msgQ.isEmpty()) {
            System.out.println("msgQ is empty");
        }
        writer.reset(msgQ.get(0).metadata.getSnapshotTimestamp());

        for (DataMessage msg : msgQ) {
            writer.apply(msg);
        }
    }

    void printTails(String tag) {
        System.out.println("\n" + tag);
        System.out.println("src dataTail " + srcDataRuntime.getAddressSpaceView().getLogTail() + " readerTail " + readerRuntime.getAddressSpaceView().getLogTail());
        System.out.println("dst dataTail " + dstDataRuntime.getAddressSpaceView().getLogTail() + " writerTail " + writerRuntime.getAddressSpaceView().getLogTail());
    }

    @Test
    public void testTwoServersCanUp () throws IOException {
        System.out.println("\ntest start");
        setupEnv();

        openStreams(srcTables, srcDataRuntime, NUM_STREAMS);
        openStreams(srcTestTables, srcTestRuntime, NUM_STREAMS);
        openStreams(dstTables, dstDataRuntime ,NUM_STREAMS);
        openStreams(dstTestTables, dstTestRuntime, NUM_STREAMS);

        // generate data at sourceServer
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        // generate data at destinationServer
        generateData(dstTables, dstHashMap, NUM_KEYS, dstDataRuntime, NUM_KEYS*2);

        verifyData(srcTables, srcHashMap);
        verifyData(srcTestTables, srcHashMap);

        verifyData(dstTables, dstHashMap);
        verifyData(dstTestTables, dstHashMap);
        return;

    }

    /**
     * Write to a corfu table and read SMRntries with streamview,
     * redirect the SMRentries to the second corfu server, and verify
     * the second corfu server contains the correct <key, value> pairs
     * @throws Exception
     */
    @Test
    public void testWriteSMREntries() throws Exception {
        // setup environment
        System.out.println("\ntest start");
        setupEnv();

        openStreams(srcTables, srcDataRuntime, NUM_STREAMS);
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);
        verifyData(srcTables, srcHashMap);

        printTails("after writing to sourceServer");
        //read streams as SMR entries
        StreamOptions options = StreamOptions.builder()
                .cacheEntries(false)
                .build();

        IStreamView srcSV = srcTestRuntime.getStreamsView().getUnsafe(CorfuRuntime.getStreamID("test0"), options);
        List<ILogData> dataList = srcSV.remaining();

        IStreamView dstSV = dstTestRuntime.getStreamsView().getUnsafe(CorfuRuntime.getStreamID("test0"), options);
        for (ILogData data : dataList) {
            OpaqueEntry opaqueEntry = OpaqueEntry.unpack(data);
            for (UUID uuid : opaqueEntry.getEntries().keySet()) {
                for (SMREntry entry : opaqueEntry.getEntries().get(uuid)) {
                    dstSV.append(entry);
                }
            }
        }

        printTails("after writing to dst");
        openStreams(dstTables, writerRuntime, NUM_STREAMS);
        verifyData(dstTables, srcHashMap);
    }

    @Test
    public void testSnapTransfer() throws IOException {
        // setup environment
        System.out.println("\ntest start ok");
        setupEnv();

        openStreams(srcTables, srcDataRuntime, NUM_STREAMS);
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);
        verifyData(srcTables, srcHashMap);

        printTails("after writing to sourceServer");

        //read snapshot from srcServer and put msgs into Queue
        readMsgs(msgQ, srcHashMap.keySet(), readerRuntime);

        long dstEntries = msgQ.size()*srcHashMap.keySet().size();
        long dstPreTail = dstDataRuntime.getAddressSpaceView().getLogTail();

        //play messages at dst server
        writeMsgs(msgQ, srcHashMap.keySet(), writerRuntime);

        long diff = dstDataRuntime.getAddressSpaceView().getLogTail() - dstPreTail;
        assertThat(diff == dstEntries);
        printTails("after writing to destinationServer");

        //verify data with hashtable
        openStreams(dstTables, dstDataRuntime, NUM_STREAMS);
        verifyData(dstTables, srcHashMap);
        System.out.println("test done");
    }


    /*
      ************** THESE TESTS RUN THROUGH THE STATE MACHINE ****************
     */

    /**
     * This test attempts to perform a snapshot sync through the Log Replication Manager.
     * We emulate the channel between source and destination by directly handling the received data
     * to the other side.
     *
     *
     *
     */
    @Test
    public void testSnapshotAndLogEntrySyncThroughManager() throws Exception {

        // Setup two separate Corfu Servers: source (primary) and destination (standby)
        setupEnv();

        // Open streams in source Corfu
        openStreams(srcTables, srcDataRuntime, NUM_STREAMS);

        // Write data into Source Tables
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        // Verify data just written against in-memory copy
        System.out.println("****** Verify Source Data");
        verifyData(srcTables, srcHashMap);

        // Before initiating log replication, verify these tables have no actual data in the destination node.
        openStreams(dstTables, dstDataRuntime, NUM_STREAMS);
        System.out.println("****** Verify No Data in Destination Site");
        verifyNoData(dstTables);

        // Start Snapshot Sync (through Source Manager)
        SourceManager logReplicationSourceManager = startSnapshotSync(srcTables.keySet());

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        verifyData(dstTables, srcHashMap);

        // Stop Replication (so we can write and control when the log entry sync starts)
        logReplicationSourceManager.stopReplication();

        // Write Extra Data (for incremental / log entry sync)
        generateTXData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS*2);
        System.out.println("****** Verify Source Data for log entry (incremental updates)");
        verifyData(srcTables, srcHashMap);

        // Reset expected messages for number of ACKs expected (log entry sync batches + 1 from snapshot sync)
        expectedAckMessages = (NUM_KEYS) + 1;

        // Start Log Entry Sync
        System.out.println("***** Start Log Entry Replication");
        logReplicationSourceManager.startReplication();

        // Block until the log entry sync completes == expected number of ACKs are received
        System.out.println("****** Wait until log entry sync completes and ACKs are received");
        blockUntilExpectedValueReached.acquire();

        // Verify Data at Destination
        System.out.println("****** Verify Destination Data for log entry (incremental updates)");
        verifyData(dstTables, srcHashMap);
    }

    /**
     * In this test we emulate the following scenario, 3 tables (T0, T1, T2). Only T1 and T2 are replicated.
     * We write following this pattern:
     *
     * - Write transactions across T0 and T1.
     * - Write individual transactions for T0 and T1
     * - Write transactions to T2 but do not replicate.
     *
     *
     * @throws Exception
     */
    @Test
    public void testSnapshotSyncCrossTables() throws Exception {

        final String t0 = TABLE_PREFIX + 0;
        final String t1 = TABLE_PREFIX + 1;
        final String t2 = TABLE_PREFIX + 2;


        // Setup two separate Corfu Servers: source (primary) and destination (standby)
        setupEnv();

        // Open streams in source Corfu
        int totalStreams = 3; // test0, test1, test2 (open stream tables)
        openStreams(srcTables, srcDataRuntime, totalStreams);

        // Write data in transaction to t0 and t1
        Set<String> crossTables = new HashSet<>();
        crossTables.add(t0);
        crossTables.add(t1);
        generateTransactionsCrossTables(srcTables, crossTables, srcHashMap, NUM_KEYS, srcDataRuntime, 0);

        // Write data to t0
        generateTransactionsCrossTables(srcTables, Collections.singleton(t0), srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        // Write data to t1
        generateTransactionsCrossTables(srcTables, Collections.singleton(t1), srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        // Write data to t2
        generateTransactionsCrossTables(srcTables, Collections.singleton(t2), srcHashMap, NUM_KEYS, srcDataRuntime, 0);

        // Write data across t0 and t1
        generateTransactionsCrossTables(srcTables, crossTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS*2);

        // Verify data just written against in-memory copy
        verifyData(srcTables, srcHashMap);

        // Before initiating log replication, verify these tables have no actual data in the destination node.
        openStreams(dstTables, dstDataRuntime, totalStreams);
        System.out.println("****** Verify No Data in Destination Site");
        verifyNoData(dstTables);

        // Start Snapshot Sync
        startSnapshotSync(crossTables);

        // Verify Data on Destination site
        System.out.println("****** Verify Data on Destination");
        // Because t2 should not have been replicated remove from expected list
        srcHashMap.get(t2).clear();
        verifyData(dstTables, srcHashMap);
    }

    private SourceManager startSnapshotSync(Set<String> tablesToReplicate) throws Exception {
        // Start Snapshot Sync (through Source Manager)
        LogReplicationConfig config = new LogReplicationConfig(tablesToReplicate, REMOTE_SITE_ID);
        SourceForwardingDataSender sourceDataSender = new SourceForwardingDataSender(SOURCE_ENDPOINT, DESTINATION_ENDPOINT, config);
        DefaultDataControl sourceDataControl = new DefaultDataControl(true);
        SourceManager logReplicationSourceManager = new SourceManager(srcTestRuntime, sourceDataSender, sourceDataControl, config);

        // Set Log Replication Source Manager so we can emulate the channel for data & control messages
        sourceDataSender.setSourceManager(logReplicationSourceManager);
        sourceDataControl.setSourceManager(logReplicationSourceManager);

        // Observe ACKs on SourceManager, to assess when snapshot sync is completed
        expectedAckMessages = 1; // We only expect one message, related to the snapshot sync complete
        ackMessages = logReplicationSourceManager.getAckMessages();
        ackMessages.addObserver(this);

        // Acquire semaphore for the first time
        blockUntilExpectedValueReached.acquire();

        // Start Snapshot Sync
        System.out.println("****** Start Snapshot Sync");
        logReplicationSourceManager.startSnapshotSync();

        // Block until the snapshot sync completes == one ACK is received by the source manager
        System.out.println("****** Wait until snapshot sync completes and ACK is received");
        blockUntilExpectedValueReached.acquire();

        return logReplicationSourceManager;
    }

    /**
     * This test attempts to perform a snapshot sync, but the log is trimmed in the middle of the process.
     * We expect the state machine to capture this event and move to the REQUIRE_SNAPSHOT_SYNC where it will
     * wait until snapshot sync is re-triggered.
     */
    @Test
    public void testSnapshotSyncWithTrimmedExceptions() throws Exception {
        // Start Snapshot Sync (we'll bypass the SourceManager and trigger it on the FSM, as we want to control params
        // in the Snapshot Sync state so we can enforce a trim on the log.
        return;
    }

    // Callback for observed values
    @Override
    public void update(Observable o, Object arg) {
        if (o == ackMessages) {
            verifyExpectedValue(ackMessages.getValue());
        }
    }

    private void verifyExpectedValue(int value) {
        // If expected value, release semaphore / unblock the wait
        // TODO (ANNY): NOT ENOUGH ONE SAME REQUEST IS SENT MULTIPLE TIMES BECAUSE NO ACK IS RECEIVED
        if (expectedAckMessages == value) {
            blockUntilExpectedValueReached.release();
        }
    }
}
