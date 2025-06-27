package com.ross.excel.serializer.avro;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import java.util.List;

import com.ross.excel.serializer.archiver.ArchiveNameResolver;
import com.ross.excel.serializer.archiver.ArchiveNameResolver.ArchiveSchedule;

public class AvroArchiveCmd<T extends SpecificRecord> {

    private final AvroDataMapper<T> avroMapper;
    private final String jobName;
    private ArchiveNameResolver.ArchiveSchedule schedule;
    private String baseDir;

    public AvroArchiveCmd(AvroDataMapper<T> mapper, String jobName, String baseDir, 
    ArchiveSchedule schedule) {
        this.avroMapper = mapper;
        this.jobName = jobName;
        this.baseDir = baseDir;
        this.schedule = schedule;
    }

    public Schema getSchema() {
        return avroMapper.getSchema();
    }

    public String getJobName() {
        return jobName;
    }

    public ArchiveNameResolver.ArchiveSchedule getArchiveSchedule() {
        return schedule;
    }   

    public String getBaseDir() {
        return baseDir;
    }

    public List<T> getRecords() {
        return avroMapper.getRecordsToArchive();
    }    

    public void setRecords (List<T> records) {
        avroMapper.setRecordsFromArchive (records);
    }
}
