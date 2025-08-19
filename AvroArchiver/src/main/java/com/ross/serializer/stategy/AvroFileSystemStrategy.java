package com.ross.serializer.stategy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AvroFileSystemStrategy<T extends SpecificRecord> extends AvroStreamingStrategy<T> {
    
    private static final Logger log = LoggerFactory.getLogger(AvroFileSystemStrategy.class);

    public AvroFileSystemStrategy(String job) {	
        super(job);
    } 

    @Override
    public <T extends SpecificRecord> void read(
        Schema schema,
        Function<T, Boolean> recordHandler
    ) throws IOException {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);

        try (SeekableInput input = new SeekableFileInput(file)) {
            super.read(schema, input, recordHandler); 
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public <T extends SpecificRecord> void readAll(
        Schema schema,
        Consumer<List<T>> recordHandler
    ) throws IOException {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);

        try (SeekableInput input = new SeekableFileInput(file)) {
            super.readAll(schema, input, recordHandler); 
        } catch (Exception e) {
            throw e; 
        }
    }

    @Override
    public <T extends SpecificRecord> void readBatched(
        Schema schema,
        Function<List<T>, Boolean> recordHandler
    ) throws IOException {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);

        try (SeekableInput input = new SeekableFileInput(file)) {
            super.readBatched(schema, input, recordHandler); 
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public <T extends SpecificRecord> void write(
        Schema schema,
        Supplier<List<T>> recordSupplier
    ) throws IOException {
        try {
            String fullPath = jobParams.getNaming();
            File file = new File(fullPath);
            super.write(schema, recordSupplier, file);

        } catch (Exception e) {
            throw e;
        }
    } 

    //@Override
    public <T extends SpecificRecord> long count(
        Class<T> clazz
    ) throws IOException {
        try {
            String fullPath = jobParams.getNaming();
            File file = new File(fullPath);

            if (!file.exists()) {
                throw new IOException("Avro file not found at: " + fullPath);
            }

            try (SeekableFileInput input = new SeekableFileInput(file)) {
                return super.fastCountRecords(clazz, input);
            }
        } catch (Exception e) {
            throw new IOException("Failed to count records", e);
        }
    }


    /*@Override
    public <T extends SpecificRecord> void write(
        Schema schema,
        Supplier<List<T>> recordSupplier
    ) throws IOException {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);
        System.out.println(fullPath);

        try {
            SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(schema);
            DataFileWriter<T> dataFileWriter = new DataFileWriter<>(writer);

            if (file.exists()) {
                dataFileWriter.appendTo(file); 
            } else {
                dataFileWriter.create(schema, file);  
            }

            List<T> batch;
            while (!(batch = recordSupplier.get()).isEmpty()) {
                for (T record : batch) {
                    dataFileWriter.append(record);
                    System.out.print(".");
                } 
                super.writeIndex(batch);
            }        

            dataFileWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }*/

    @Override
    public <T extends SpecificRecord> void findMatchingRecords(
        Class<T> clazz,
        Predicate<T> recordFilter,
        Function<T, Boolean> onMatch
    ) throws Exception {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);

        try (SeekableInput input = new SeekableFileInput(file)) {
            super.findMatchingRecords(clazz, input, recordFilter, onMatch);
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Moves all archived files from the root directory to the archive directory, 
     * excluding the current archive file.
     * <p>
     * Files are identified by their naming format, and any existing files in the 
     * archive directory with the same name will be overwritten.
     *
     * @param name    the current archive file name to exclude from moving
     * @return {@code true} if all applicable files were moved successfully; 
     *         {@code false} otherwise
     */    
    @Override
    public boolean put(String name) {
        String baseDir = jobParams.getStorage().getFile().getBaseDir();
        String archiveDir = jobParams.getStorage().getFile().getArchiveDir();

        File root = new File(baseDir);
        List<String> archives = getNames(baseDir);

        // 3. Copy completed archives to archiveDir
        for (String archiveName : archives) {
            File source = new File(root, archiveName);
            File target = new File(archiveDir, archiveName);

            try {
                Files.createDirectories(target.getParentFile().toPath());
                Files.copy(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
                log.debug("Archived: " + archiveName);
            } catch (IOException e) {
                log.error("Failed to copy " + archiveName + ": " + e.getMessage());
                return false;
            }
        }
        return true;
    }

    private String stripAvroSuffix(String fileName) {
        return fileName.toLowerCase().endsWith(".avro")
            ? fileName.substring(0, fileName.length() - 5)
            : fileName;
    }

    @Override
	public boolean get(String name) {
        return false;
    }

    @Override
    public List<String> getNames(String reference) {
        //String baseDir = jobParams.getStorage().getFile().getBaseDir();
        String baseName = stripAvroSuffix(jobParams.getJob().getFileName());
        ArchiveJobParams.ArchiveSchedule schedule = jobParams.getJob().getArchiveNamingScheme();

        ArchiveJobParams.ArchiveNameResolver resolver = jobParams.getArchiveNameResolver();
        String currentArchive = resolver.resolveAvroArchiveFileName(baseName, schedule);

        // 1. List all archives in baseDir matching the "<baseName>-*.avro" pattern
        File root = new File(reference);
        if (!root.exists() || !root.isDirectory()) {
            log.error("Directory not found: " + reference);
            return new ArrayList<>();
        }

        List<String> allArchives = Arrays.stream(
            root.list((dir, filename) -> 
                filename.startsWith(baseName + "-") && filename.endsWith(".avro")
            )
        ).collect(Collectors.toList());

        // 2. Filter completed archives using your resolver
        List<String> completedArchives = resolver.findCompletedArchives(allArchives, baseName, schedule);
        return completedArchives;
      }

}
