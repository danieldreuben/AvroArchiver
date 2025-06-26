package com.ross.excel.serializer.mapper;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import java.util.List;

//@FunctionalInterface
public interface AvroDataMapper {
    List<SpecificRecord> getRecords();
    Schema getSchema();
}

