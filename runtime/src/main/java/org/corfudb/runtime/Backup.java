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
    String tmpDirPath;
    List<UUID> streamIDs;
    long timestamp;
    CorfuRuntime runtime;

    public Backup(String filePath, List<UUID> streamIDs, CorfuRuntime runtime) {
        this.filePath = filePath;
        this.tmpDirPath = filePath + "/tmp";
        this.streamIDs = streamIDs;
        this.runtime = runtime;
        this.timestamp = runtime.getAddressSpaceView().getLogTail();
    }

    public boolean start() throws IOException {
        File filePathDir = new File(filePath);
        if (!filePathDir.exists() && !filePathDir.mkdir()) {
            return false;
        }
        File tmpDir = new File(tmpDirPath);
        if (!tmpDir.exists() && !tmpDir.mkdir()) {
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
            String fileName = tmpDirPath + File.separator + streamId;
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
     * Merge table backups into a single tar file
     */
    public void mergeIntoTarFile() {
        File folder = new File(tmpDirPath);
        if (!folder.isDirectory())
            return;

        File[] srcFiles = folder.listFiles();
        if (srcFiles == null)
            return;

        byte[] buf = new byte[1024];
        try {
            FileOutputStream fileOutput = new FileOutputStream(filePath + "/backup.tar");
            TarArchiveOutputStream TarOutput = new TarArchiveOutputStream(fileOutput);

            for (File srcFile : srcFiles) {
                FileInputStream fileInput = new FileInputStream(srcFile);
                TarArchiveEntry tarEntry = new TarArchiveEntry(srcFile);
                tarEntry.setName(srcFile.getName());
                TarOutput.putArchiveEntry(tarEntry);
                int num;
                while ((num = fileInput.read(buf, 0, 1024)) != -1) {
                    TarOutput.write(buf, 0, num);
                }
                TarOutput.closeArchiveEntry();
                fileInput.close();
            }
            TarOutput.close();
            fileOutput.close();
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    /**
     * All generated files under tmp directory will be composed into one .tar file
     */
    public void generateTarFile() throws IOException {
        backup();
        mergeIntoTarFile();
    }

    /**
     * It is called when it failed to backupTable one of the table, the whole backupTable process will fail and
     * cleanup the files under the tmp directory.
     */
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(tmpDirPath));
    }
}
