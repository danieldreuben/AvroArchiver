package com.ross.excel.serializer.archiver;

import java.io.IOException;
import java.util.List;

import org.apache.avro.specific.SpecificRecord;

public interface ArchiverStrategy {
    void archive() throws IOException;
    
    //List<SpecificRecord> read(SpecificDatumReader<? extends SpecificRecord> reader) throws IOException;
    List<SpecificRecord> read() throws IOException;

}