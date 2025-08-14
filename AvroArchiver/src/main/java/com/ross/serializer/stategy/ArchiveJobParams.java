package com.ross.serializer.stategy;


import java.io.File;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ArchiveJobParams {

    private Job job;
    private Storage storage;
    private Indexer indexer;

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

    }

    public static ArchiveJobParams getInstance(String job) {
        try {       
            if (job.contains(".")) {
                File file = new File(job);
                ArchiveJobParams params = new ArchiveJobParams();
                params.getJob().setFileName(file.getName());
                //params.getStorage().setFile(file.getParent() != null ? file.getParent() : "");
                return params;
            }                   
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            String fullPath = job + ".yaml";
            System.out.println(">>>"+fullPath);
            ArchiveJobParams config = mapper.readValue(new File(fullPath), ArchiveJobParams.class);
          
            return config;    

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public ArchiveNameResolver getArchiveNameResolver() {
        return new ArchiveNameResolver();
    }

    public String getNaming() {
        return (job.archiveNamingScheme == null) ? 
            getJob().getFileName() :
                new ArchiveNameResolver().
                resolveAvroArchiveFileName(getJob().getFileName(), getJob().archiveNamingScheme);            
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
        //private String type;
        private FileStorage file;
        private BlobStorage blob;

        /*public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }*/

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
        
            private String baseDir;
            private String archiveDir;
            
            public String getBaseDir() {
                return baseDir;
            }

            public void setBaseDir(String baseDir) {
                this.baseDir = baseDir;
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
            return namingResolver(baseName, schedule, ZonedDateTime.now());
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
