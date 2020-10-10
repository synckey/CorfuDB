package org.corfudb.runtime;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.OpaqueStream;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

@Slf4j
public class Backup {

    // The path of backup tar file
    private final String filePath;

    // The path of a temporary directory under which table's backup files are stored
    private final String backupTempDirPath;

    // The stream IDs of tables which are backed up
    private final List<UUID> streamIDs;

    // The snapshot address to back up
    private final long timestamp;

    // The Corfu Runtime which is performing the back up
    private final CorfuRuntime runtime;

    /**
     * Backup files of tables are temporarily stored under BACKUP_TEMP_DIR. They are deleted after backup finishes.
     */
    private static final String BACKUP_TEMP_DIR_PREFIX = "corfu_backup_";


    public Backup(String filePath, List<UUID> streamIDs, CorfuRuntime runtime) throws IOException {
        this.filePath = filePath;
        this.backupTempDirPath = Files.createTempDirectory(BACKUP_TEMP_DIR_PREFIX).toString();
        this.streamIDs = streamIDs;
        this.runtime = runtime;
        this.timestamp = runtime.getAddressSpaceView().getLogTail();
    }

    public boolean start() throws IOException {
        if (!backup()) {
            cleanup();
            return false;
        }

        generateTarFile();
        cleanup();
        return true;
    }

    /**
     * All temp backupTable files will be put at BACKUP_DIR_PATH directory.
     * @return
     */
    private boolean backup() throws IOException {
        for (UUID streamId : streamIDs) {
            String fileName = backupTempDirPath + File.separator + streamId;
            if (!backupTable(fileName, streamId)) {
                return false;
            }
        }

        return true;
    }

    /**
     * If th log is trimmed at timestamp, the backupTable will fail.
     * If the table has no data to backupTable, it will create a file with empty contents.
     * @param fileName
     * @param uuid
     */
    private boolean backupTable(String fileName, UUID uuid) throws IOException, TrimmedException {
        FileOutputStream fileOutput = new FileOutputStream(fileName);
        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(false)
                .cacheEntries(false)
                .build();

        Stream stream = (new OpaqueStream(runtime, runtime.getStreamsView().get(uuid, options))).streamUpTo(timestamp);
        Iterator iterator = stream.iterator();

        while (iterator.hasNext()) {
            OpaqueEntry lastEntry = (OpaqueEntry) iterator.next();
            List<SMREntry> smrEntries = lastEntry.getEntries().get(uuid);
            if (smrEntries != null) {
                Map<UUID, List<SMREntry>> map = new HashMap<>();
                map.put(uuid, smrEntries);
                OpaqueEntry newOpaqueEntry = new OpaqueEntry(lastEntry.getVersion(), map);
                OpaqueEntry.write(fileOutput, newOpaqueEntry);
            }
        }

        fileOutput.flush();
        fileOutput.close();
        return true;
    }

    /**
     * All generated files under tmp directory will be composed into one .tar file
     */
    private void generateTarFile() throws IOException {
        File folder = new File(backupTempDirPath);
        File[] srcFiles = folder.listFiles();

        FileOutputStream fileOutput = new FileOutputStream(filePath);
        TarArchiveOutputStream tarOutput = new TarArchiveOutputStream(fileOutput);

        int count;
        byte[] buf = new byte[1024];
        for (File srcFile : srcFiles) {
            FileInputStream fileInput = new FileInputStream(srcFile);
            TarArchiveEntry tarEntry = new TarArchiveEntry(srcFile);
            tarEntry.setName(srcFile.getName());
            tarOutput.putArchiveEntry(tarEntry);

            while ((count = fileInput.read(buf, 0, 1024)) != -1) {
                tarOutput.write(buf, 0, count);
            }
            tarOutput.closeArchiveEntry();
            fileInput.close();
        }
        tarOutput.close();
        fileOutput.close();
    }

    /**
     * Cleanup the table backup files under the backupDir directory.
     */
    private void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(backupTempDirPath));
    }
}
