package com.ross.excel.serializer.avro;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import java.util.List;

import com.ross.excel.serializer.archiver.ArchiveNameResolver;
import com.ross.excel.serializer.archiver.ArchiveNameResolver.ArchiveSchedule;

public class ArchiveCommand<T extends SpecificRecord> {

    //private final AvroDataMapper<T> avroMapper;
    private final String jobName;
    private ArchiveNameResolver.ArchiveSchedule schedule;
    private String baseDir;
    private int readBatchSize=10; 
    private int writeBatchSize=10;

    public ArchiveCommand(String jobName, String baseDir, ArchiveSchedule schedule) {
        //this.avroMapper = mapper;
        this.jobName = jobName;
        this.baseDir = baseDir;
        this.schedule = schedule;
    }

    /*public Schema getSchema() {
        //return avroMapper.getSchema();
        T.getSchema();
    }*/

    public String getJobName() {
        return jobName;
    }

    public int getReadBatchSize() {
        return readBatchSize;
    }
    
    public int getWriteBatchSize() {
        return writeBatchSize;
    }  

   public void setReadBatchSize(int t) {
        this.readBatchSize = t;
    }
    public void setWriteBatchSize(int t) {
        this.writeBatchSize = t;
    } 

    public ArchiveNameResolver.ArchiveSchedule getArchiveSchedule() {
        return schedule;
    }   

    public String getBaseDir() {
        return baseDir;
    }
/* 
    public List<T> getRecords() {
        return avroMapper.getRecordsToArchive();
    }    

    public void setRecords (List<SpecificRecord> records) {
        avroMapper.setRecordsFromArchive (records);
    } */
}
