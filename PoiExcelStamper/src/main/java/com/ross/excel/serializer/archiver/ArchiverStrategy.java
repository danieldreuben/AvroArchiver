package com.ross.excel.serializer.archiver;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public interface ArchiverStrategy {
    
    public <T extends SpecificRecord> void readBatches(
		Schema schema,
		Function<List<T>, Boolean> recordHandler // return false to stop
	) throws IOException ;

	public <T extends SpecificRecord> void write(
            Schema schema,        
		    Supplier<List<T>> recordSupplier
    ) throws IOException;    

    public <T extends SpecificRecord> void read(
		Schema schema,
		Function<T, Boolean> recordHandler // return false to stop
	) throws IOException; 

	public <T extends SpecificRecord> void readAll(
            Schema schema,       
		    Consumer<List<T>> recordHandler
	) throws IOException;

    /*public <T extends SpecificRecord> void findAllMatchingRecords(
		    Schema schema,
		    Predicate<T> recordFilter,
		    Consumer<T> recordHandler
	) throws IOException;*/

	public <T extends SpecificRecord> Optional<T> findMatchingRecords(
		Schema schema,
		Predicate<T> recordFilter
	) throws IOException;    
}