package com.ross.excel.serializer.avro;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import com.ross.excel.serializer.archiver.DataMapper;

//@FunctionalInterface
/** public interface AvroDataMapper {
    List<SpecificRecord> getRecords();
    Schema getSchema();
}*/


public interface AvroDataMapper<T extends SpecificRecord> extends DataMapper<T> {
    //@Override
    Schema getSchema(); // narrows Object to Avro Schema
    //@Override
    //<T extends SpecificRecord> getRcords();
    //List<T> getRecords();
    //@Override
    //void setRecordsFromArchive(List<T> t); 
    
}