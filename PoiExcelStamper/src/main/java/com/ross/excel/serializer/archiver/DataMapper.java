package com.ross.excel.serializer.archiver;

import java.util.List;

import org.apache.avro.specific.SpecificRecord;

public interface DataMapper<T> {
    List<T> getRecordsToArchive();
    //void setRecordsFromArchive(List<T> records);
    //void setRecordsFromArchive();
    //Object getSchema(); // can return any schema/metadata representation
}

