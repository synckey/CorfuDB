package org.corfudb;

import static org.corfudb.infrastructure.BatchWriterOperation.Type.WRITE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.BatchProcessor;
import org.corfudb.infrastructure.LogUnitServer.LogUnitServerConfig;
import org.corfudb.infrastructure.PerformantBatchWriter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.util.serializer.Serializers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class BatchWriterTest {
    private static final int TOTAL_RECORDS = 1_000 * 1_000;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final ServerContextBuilder serverContextBuilder = new ServerContextBuilder();

    private ServerContext serverContext;
    private StreamLogFiles streamLog;

    private ServerContext getContext() {
        String path = folder.getRoot().getAbsolutePath();
        return serverContextBuilder
                .setLogPath(path)
                .setMemory(false)
                .build();
    }

    @Test
    public void oldPerf() throws Exception {
        serverContext = getContext();
        streamLog = new StreamLogFiles(serverContext, true);

        BatchProcessor batchWriter = new BatchProcessor(streamLog, serverContext.getServerEpoch(), true);

        final long start = System.currentTimeMillis();

        for (int i = 0; i < TOTAL_RECORDS; i++) {
            // Enable checksum, then append and read the same entry
            final long addr = i;
            LogData entry = buildLogData(addr);
            CorfuPayloadMsg<WriteRequest> writeReq = new CorfuPayloadMsg<>(CorfuMsgType.WRITE, new WriteRequest(entry));
            batchWriter.addTask(WRITE, writeReq);
        }

        batchWriter.stopProcessor();

        long total = System.currentTimeMillis() - start;
        System.out.println("queue size " + batchWriter.operationsQueue.size());
        System.out.println("time: " + total);
    }

    @Test
    public void perf() throws InterruptedException {
        serverContext = getContext();
        streamLog = new StreamLogFiles(serverContext, true);

        //BatchWriter<Long, LogData> batchWriter = new BatchWriter<>(streamLog, 0, true);
        PerformantBatchWriter<Long, LogData> batchWriter = new PerformantBatchWriter<>(streamLog, 0, true);

        LogUnitServerConfig luCfg = LogUnitServerConfig.parse(serverContext.getServerConfig());

        /**LoadingCache<Long, LogData> dataCache = Caffeine.newBuilder()
         .<Long, LogData>weigher((k, v) -> v.getData() == null ? 1 : v.getData().length)
         .maximumWeight(luCfg.getMaxCacheSize())
         //.removalListener(this::handleEviction)
         .writer(batchWriter)
         .build(this::handleRetrieval);**/

        long start = System.currentTimeMillis();

        final AtomicLong records = new AtomicLong();

        for (int i = 0; i < TOTAL_RECORDS; i++) {
            // Enable checksum, then append and read the same entry
            final long addr = i;
            LogData entry = buildLogData(addr);

            batchWriter.asyncWrite(entry, op -> {
                //ignore
                records.incrementAndGet();
            });
        }

        while (records.get() < TOTAL_RECORDS) {
            final int timeout = 100;
            Thread.sleep(timeout);
        }

        batchWriter.close();
        System.out.println("Wait for termination");
        final int ms = 50;
        batchWriter.getWriterService().awaitTermination(ms, TimeUnit.MILLISECONDS);

        long total = System.currentTimeMillis() - start;
        System.out.println("time: " + total);
        //System.out.println("Speed: " + totalRecords / streamLog.avgTime.stream().mapToLong(l -> l).sum() * 1000);

        final int timeout = 100000000;
        //Thread.sleep(timeout);
    }

    private static final byte[] streamEntry = ("PayloadPayloadPayloadPayloadPayloadPayloadPayloadPayloadPayloadPay")
            .getBytes();
    private static final ByteBuf b = Unpooled.buffer();

    static {
        Serializers.CORFU.serialize(streamEntry, b);
    }

    private LogData buildLogData(long address) {
        LogData data = new LogData(DataType.DATA, b);
        data.setEpoch(0L);
        data.setGlobalAddress(address);

        return data;
    }

    private synchronized LogData handleRetrieval(long address) {
        LogData entry = streamLog.read(address);
        log.trace("Retrieved[{} : {}]", address, entry);
        return entry;
    }
}