package com.ross.excel.serializer.archiver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ArchiveJobParams {
    private String description;
    private ArchiveSchedule namingSchedule;
    private String fileName;
    private String baseDir;
    private String container;
    private String endpoint;
    private int deflate = 0; // max 9 = off
    private int batchRead = 20;
    private int batchWrite = 20;
    

    public ArchiveJobParams() {
    }

    public static ArchiveJobParams getInstance(String job) {
        try {
            //System.out.println("job: " + job);
            if (job.contains(".")) {
                File file = new File(job);
                ArchiveJobParams params = new ArchiveJobParams();
                params.setFileName(file.getName());
                params.setBaseDir(file.getParent() != null ? file.getParent() : "");
                return params;
            }
        
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            String resourcePath = "/" + job + ".yaml"; // Add leading slash for classpath lookup

            InputStream is = ArchiveJobParams.class.getResourceAsStream(resourcePath);
            if (is == null) {
                throw new FileNotFoundException("YAML file not found on classpath: " + resourcePath);
            } 

            ArchiveJobParams params = mapper.readValue(is, ArchiveJobParams.class);
            return params;

        } catch (Exception e) {
            throw new RuntimeException("Failed to load job config for: " + job, e);
        }
    } 
 
    public String getDescription() {
        return this.description;
    }

    public String getFileName() {
        return this.fileName;
    }

    public int getDeflate() {
        return this.deflate;
    }

    public ArchiveSchedule getNamingSchedule() {
        return this.namingSchedule;
    }

    public String getBaseDir() {
        return this.baseDir;
    }

    public String getFullPath() {
        return this.baseDir + this.fileName;
    }

    public int getBatchRead() {
        return this.batchRead;
    }

    public int getBatchWrite() {
        return this.batchWrite;
    }

    public String getFileNamingSchedule() {
        
        return (namingSchedule == null) ? 
            getBaseDir()+getFileName() :
                new ArchiveNameResolver()
                    .resolveAvroArchiveFileName(getBaseDir()+getFileName(), namingSchedule);
    }
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
    public void setDescription(String description) {
        this.description = description;
    }

    public void setFileName(String filename) {
        this.fileName = filename;
    }

    public void setNamingSchedule(ArchiveSchedule namingSchedule) {
        this.namingSchedule = namingSchedule;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }    

    public void setBatchRead(int t) {
        this.batchRead = t;
    }
    public void setBatchWrite(int t) {
        this.batchWrite = t;
    }

    public void setDeflate(int t) {
        deflate = t;
    }

    public enum ArchiveSchedule {
        YEARLY,
        MONTHLY,
        WEEKLY,
        DAILY,
        HOURLY
    }

    @Override
    public String toString() {
        return "job: " + this.description + " fileName:" + this.fileName + " schedule:" + namingSchedule;
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
    }
}


