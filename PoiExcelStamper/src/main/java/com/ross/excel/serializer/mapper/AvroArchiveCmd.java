package com.ross.excel.serializer.mapper;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import java.util.List;

import com.ross.excel.serializer.mapper.ArchiveNameResolver.ArchiveSchedule;

public class AvroArchiveCmd {

    private final AvroDataMapper avroMapper;
    private final String jobName;
    private ArchiveNameResolver.ArchiveSchedule schedule;
    private String baseDir;

    public AvroArchiveCmd(AvroDataMapper mapper, String jobName, String baseDir, 
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

    public List<SpecificRecord> getRecords() {
        return avroMapper.getRecords();
    }    
}
