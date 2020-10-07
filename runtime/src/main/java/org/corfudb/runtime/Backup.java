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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

@Slf4j
public class Backup {

    String filePath;
    String backupDirPath;
    List<UUID> streamIDs;
    long timestamp;
    CorfuRuntime runtime;

    /**
     * Backup files of tables are temporarily stored under this directory. They are deleted after backup finishes.
     */
    public static final String BACKUP_DIR_RELATIVE_PATH = "tmp";

    /**
     * Pack table backup files into a single tar file
     */
    public static final String BACKUP_TAR_FILENAME = "backup.tar";


    public Backup(String filePath, List<UUID> streamIDs, CorfuRuntime runtime) {
        this.filePath = filePath;
        this.backupDirPath = filePath + File.separator + BACKUP_DIR_RELATIVE_PATH;
        this.streamIDs = streamIDs;
        this.runtime = runtime;
        this.timestamp = runtime.getAddressSpaceView().getLogTail();
    }

    public boolean start() throws IOException {
        File filePathDir = new File(filePath);
        if (!filePathDir.exists() && !filePathDir.mkdirs()) {
            return false;
        }

        File backupDir = new File(backupDirPath);
        if (!backupDir.exists() && !backupDir.mkdirs()) {
            return false;
        }

        if (!backup()) {
            cleanup();
            return false;
        }

        generateTarFile();
        cleanup();
        return true;
    }

    /**
     * All temp backupTable files will be put at filePath/tmp directory.
     * @return
     */
    public boolean backup() throws IOException {
        for (UUID streamId : streamIDs) {
            String fileName = backupDirPath + File.separator + streamId;
            if (!backupTable(fileName, streamId, runtime, timestamp)) {
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
    public static boolean backupTable(String fileName, UUID uuid, CorfuRuntime runtime, long timestamp) throws IOException, TrimmedException {
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
    public void generateTarFile() throws IOException {
        File folder = new File(backupDirPath);
        File[] srcFiles = folder.listFiles();

        FileOutputStream fileOutput = new FileOutputStream(filePath + File.separator + BACKUP_TAR_FILENAME);
        TarArchiveOutputStream TarOutput = new TarArchiveOutputStream(fileOutput);

        int count;
        byte[] buf = new byte[1024];
        for (File srcFile : srcFiles) {
            FileInputStream fileInput = new FileInputStream(srcFile);
            TarArchiveEntry tarEntry = new TarArchiveEntry(srcFile);
            tarEntry.setName(srcFile.getName());
            TarOutput.putArchiveEntry(tarEntry);

            while ((count = fileInput.read(buf, 0, 1024)) != -1) {
                TarOutput.write(buf, 0, count);
            }
            TarOutput.closeArchiveEntry();
            fileInput.close();
        }
        TarOutput.close();
        fileOutput.close();
    }

    /**
     * Cleanup the table backup files under the backupDir directory.
     */
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(backupDirPath));
    }
}
