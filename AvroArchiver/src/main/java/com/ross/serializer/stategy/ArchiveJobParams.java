package com.ross.serializer.stategy;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ArchiveJobParams {
	private static final Logger log = LoggerFactory.getLogger(ArchiveJobParams.class);
    private Job job;
    private Storage storage;
    private Indexer indexer;
    private ArchiveNameResolver nameResolver;

     public enum ArchiveSchedule {
        YEARLY,
        MONTHLY,
        WEEKLY,
        DAILY,
        HOURLY
    }

    public ArchiveJobParams() {
        job = new Job();
        storage = new Storage();
        storage.setFile(new Storage.FileStorage());
        nameResolver = new ArchiveNameResolver();
    }

    public static ArchiveJobParams getInstance(String job) 
    throws Exception {
        try {
            if (job == null || job.isEmpty()) {
                throw new IllegalArgumentException("job must not be null/empty");
            }

            // Case 1: job contains '.' -> explicit file (e.g. *.avro)
            if (job.contains(".")) {
                File file = new File(job);
                ArchiveJobParams params = new ArchiveJobParams();
                params.getJob().setFileName(file.getName());
                params.getStorage().getFile().setWorkDir(file.getParent() != null ? file.getParent() : "");  
                //System.out.println("workdir: " + file.getParent());     
                return params;
            }

            // Case 2: job name -> load <job>.yaml from classpath
            String resourceName = job + ".yaml";
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

            try (InputStream is = Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResourceAsStream(resourceName)) {
                if (is == null) {
                    throw new FileNotFoundException("Resource not found in classpath: " + resourceName);
                }
                log.debug("Loading job params from classpath: {}", resourceName);
                return mapper.readValue(is, ArchiveJobParams.class);
            }

        } catch (Exception e) {
            log.error("Failed to load job params for '{}': {}", job, e.getMessage(), e);
            throw e;
        }
    }

    public ArchiveNameResolver getArchiveNameResolver() {
        return nameResolver;
    }

    public String getNaming() {
        return (job.archiveNamingScheme == null) ? 
            getJob().getFileName() :
                nameResolver.
                resolveAvroArchiveFileName(getJob().getFileName(), getJob().archiveNamingScheme);            
    }

    public String getFullPath() {
        return getStorage().getFile().getWorkDir() + "/" + getNaming();
    }

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

     public Indexer getIndexer() {
        return indexer;
    }

    public void setIndexer(Indexer indexer) {
        this.indexer = indexer;
    }   

    public static class Job {
        private String description;
        private ArchiveSchedule archiveNamingScheme;
        private String fileName;
        private int deflate;
        private int batchRead;

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.
            description = description;
        }

        public ArchiveSchedule getArchiveNamingScheme() {
            return archiveNamingScheme;
        }

        public void setArchiveNamingScheme(ArchiveSchedule archiveNamingScheme) {
            this.archiveNamingScheme = archiveNamingScheme;
        }  

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public int getDeflate() {
            return deflate;
        }

        public void setDeflate(int deflate) {
            this.deflate = deflate;
        }

        public int getBatchRead() {
            return batchRead;
        }

        public void setBatchRead(int batchRead) {
            this.batchRead = batchRead;
        }
    }

    public static class Storage {
        private FileStorage file;
        private BlobStorage blob;

        public FileStorage getFile() {
            return file;
        }

        public void setFile(FileStorage file) {
            this.file = file;
        }

        public BlobStorage getBlob() {
            return blob;
        }

        public void setBlob(BlobStorage blob) {
            this.blob = blob;
        }

        public static class FileStorage {
        
            private String workDir;
            private String archiveDir;
            
            public String getWorkDir() {
                return workDir;
            }

            public void setWorkDir(String workDir) {
                this.workDir = workDir;
            }
            public String getArchiveDir() {
                return archiveDir;
            }

            public void setArchiveDir(String archiveDir) {
                this.archiveDir = archiveDir;
            }            
        }

        public static class BlobStorage {
            private String endpoint;
            private String container;

            public String getEndpoint() {
                return endpoint;
            }

            public void setEndpoint(String endpoint) {
                this.endpoint = endpoint;
            }

            public String getContainer() {
                return container;
            }

            public void setContainer(String container) {
                this.container = container;
            }
        }
    }

    public class Indexer {
        private String name;
        private String method;

        // Getters and Setters
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getMethod() { return method; }
        public void setMethod(String method) { this.method = method; }
    }

    public class ArchiveNameResolver {

        public String resolveAvroArchiveFileName(String baseName, ArchiveSchedule schedule) {
            String name = namingResolver(baseName, schedule, ZonedDateTime.now());
            return name;
        }

        private String namingResolver(String baseName, ArchiveSchedule schedule, ZonedDateTime dateTime) {
            // Strip ".avro" suffix if present (case-insensitive)
            if (baseName.toLowerCase().endsWith(".avro")) {
                baseName = baseName.substring(0, baseName.length() - 5);
            }

            String suffix;
            switch (schedule) {
                case YEARLY:
                    suffix = dateTime.format(DateTimeFormatter.ofPattern("uuuu"));
                    break;
                case MONTHLY:
                    suffix = dateTime.format(DateTimeFormatter.ofPattern("uuuuMM"));
                    break;
                case WEEKLY:
                    suffix = dateTime.format(DateTimeFormatter.ofPattern("uuuu'W'ww"));
                    break;
                case DAILY:
                    suffix = dateTime.format(DateTimeFormatter.ofPattern("uuuuMMdd"));
                    break;
                case HOURLY:
                    suffix = dateTime.format(DateTimeFormatter.ofPattern("uuuuMMdd-HH"));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported schedule: " + schedule);
            }

            return baseName + "-" + suffix + ".avro";
        }

        public List<String> findCompletedArchives(
                List<String> archiveNames,
                String baseName,
                ArchiveSchedule schedule) {

            ZonedDateTime now = ZonedDateTime.now();
            List<String> completed = new ArrayList<>();

            // Current period token based on schedule
            String currentToken;
            switch (schedule) {
                case YEARLY:
                    currentToken = now.format(DateTimeFormatter.ofPattern("uuuu"));
                    break;
                case MONTHLY:
                    currentToken = now.format(DateTimeFormatter.ofPattern("uuuuMM"));
                    break;
                case WEEKLY:
                    currentToken = now.format(DateTimeFormatter.ofPattern("uuuu'W'ww"));
                    break;
                case DAILY:
                    currentToken = now.format(DateTimeFormatter.ofPattern("uuuuMMdd"));
                    break;
                case HOURLY:
                    currentToken = now.format(DateTimeFormatter.ofPattern("uuuuMMdd-HH"));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported schedule: " + schedule);
            }

            for (String name : archiveNames) {
                String periodToken = name
                        .replace(baseName + "-", "")
                        .replace(".avro", "")
                        .trim();

                // If it's not the current period, it's completed
                if (!periodToken.equals(currentToken)) {
                    completed.add(name);
                }
            }

            return completed;
        }
    }
   
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append("description='").append(this.getJob().getDescription()).append('\'');
        sb.append(", archiveNamingScheme='").append(this.getJob().getArchiveNamingScheme()).append('\'');
        sb.append(", fileName='").append(this.getJob().getFileName()).append('\'');
        sb.append(", deflate=").append(this.getJob().getDeflate());
        sb.append(", batchRead=").append(this.getJob().getBatchRead());
        sb.append('}');
        return sb.toString();
    }

}
