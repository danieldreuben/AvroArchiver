package com.ross.serializer.stategy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificRecord;

public interface ArchiverStrategy {
  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
    public abstract <T extends SpecificRecord> void read(
		Schema schema,
		Function<T, Boolean> recordHandler 
	) throws IOException; 

	public abstract <T extends SpecificRecord> void write(
        Schema schema,        
		Supplier<List<T>> recordSupplier
    ) throws IOException;    
	
	public <T extends SpecificRecord> void write(
		Schema schema,
		Supplier<List<T>> recordSupplier,
		File file		
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

	public ArchiveJobParams getJobParams();
}