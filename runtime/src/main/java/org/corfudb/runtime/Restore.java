package org.corfudb.runtime;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.util.serializer.Serializers;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class Restore {
    String filePath;
    String tableDirPath;
    List<UUID> streamIDs;
    CorfuRuntime runtime;
    CorfuStore corfuStore;

    public Restore(String filePath, List<UUID> streamIDs, CorfuRuntime runtime) {
        this.filePath = filePath;
        this.streamIDs = streamIDs;
        this.runtime = runtime;
        this.corfuStore = new CorfuStore(runtime);
    }

    public boolean start() throws IOException {
        File backupTarFile = new File(filePath);
        if (!backupTarFile.exists()) {
            return false;
        }
        File parentFile = backupTarFile.getParentFile();
        if (!parentFile.exists() && !parentFile.mkdirs()) {
            return false;
        }
        tableDirPath = parentFile.getPath() + File.separator + "/table_backups";
        new File(tableDirPath).mkdir();

        openTarFile();

        if (!verify()) {
            return false;
        };

        restore();
        return true;
    }

    public boolean restore() throws IOException {
        for (UUID streamId : streamIDs) {
            String fileName = tableDirPath + File.separator + streamId;
            if (!restoreTable(fileName, streamId, streamId, corfuStore)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Read the file generated by backupTable
     * @param fileName
     * @param streamId
     * @return
     */
    public static boolean restoreTable(String fileName, UUID streamId, UUID srcStreamId, CorfuStore corfuStore) throws IOException {
        FileInputStream fileInput = new FileInputStream(fileName);
        long numEntries = 0;
        Path path = Paths.get(fileName);

        try {

            TxBuilder tx = corfuStore.tx(CORFU_SYSTEM_NAMESPACE);
            SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
            tx.logUpdate(streamId, entry);
            tx.commit();

            /**
             * For each opaque entry , write a transaction to the database.
             */
            while (fileInput.available()> 0) {
                OpaqueEntry opaqueEntry = OpaqueEntry.read(fileInput);
                List<SMREntry> smrEntries = opaqueEntry.getEntries().get(srcStreamId);
                if (smrEntries == null || smrEntries.isEmpty()) {
                    continue;
                }

                CorfuStoreMetadata.Timestamp ts = corfuStore.getTimestamp();
                TxBuilder txBuilder = corfuStore.tx(CORFU_SYSTEM_NAMESPACE);
                txBuilder.logUpdate(streamId, smrEntries);
                txBuilder.commit(ts);
                numEntries++;
                log.debug("write uuid {} src uuid {} with numEntries", streamId, numEntries);
            }
        } catch (Exception e) {
            log.error("catch an exception ", e);
            throw e;
        }

        fileInput.close();
        return true;
    }

    /**
     * Open the given backup tar file and save the table's backup to /tmp
     * */
    public void openTarFile() throws IOException {
        FileInputStream fileInput = new FileInputStream(filePath);
        TarArchiveInputStream TarInput = new TarArchiveInputStream(fileInput);

        int count;
        byte data[] = new byte[1024];
        TarArchiveEntry entry;
        while ((entry = TarInput.getNextTarEntry()) != null) {
            String tablePath = tableDirPath + File.separator + entry.getName();
            FileOutputStream fos = new FileOutputStream(tablePath, false);
            try (BufferedOutputStream dest = new BufferedOutputStream(fos, 1024)) {
                while ((count = TarInput.read(data, 0, 1024)) != -1) {
                    dest.write(data, 0, count);
                }
            }
        }
    }

    /**
     * Some verifications such as
     * - Compare the user provided streamIds and names of table backups under tmp directory, or some metadata file
     * - Checksum
     * */
    public boolean verify() {
        return true;
    }
}
