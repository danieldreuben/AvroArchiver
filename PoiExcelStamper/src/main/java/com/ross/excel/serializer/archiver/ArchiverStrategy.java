package com.ross.excel.serializer.archiver;

import java.io.File;
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

    public abstract <T extends SpecificRecord> void read(
		Schema schema,
		Function<T, Boolean> recordHandler 
	) throws IOException; 

	public abstract <T extends SpecificRecord> void write(
        Schema schema,        
		Supplier<List<T>> recordSupplier
    ) throws IOException;    
	
	public <T extends SpecificRecord> void write2(
		Schema schema,
		File file,
		Supplier<List<T>> recordSupplier
	) throws IOException;

	public abstract <T extends SpecificRecord> Optional<T> find(
		Schema schema,
		Predicate<T> recordFilter
	) throws IOException;  

	public abstract <T extends SpecificRecord> void readAll(
        Schema schema,       
		Consumer<List<T>> recordHandler
	) throws IOException;

    public abstract <T extends SpecificRecord> void readBatched(
		Schema schema,
		Function<List<T>, Boolean> recordHandler 
	) throws IOException ;

	public abstract <T extends SpecificRecord> void findMatchingRecords(
		Class<T> clazz,
		Predicate<T> recordFilter,
		Function<T, Boolean> onMatch
	) throws Exception;
}