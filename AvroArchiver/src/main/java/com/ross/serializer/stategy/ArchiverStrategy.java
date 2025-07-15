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
import org.apache.avro.specific.SpecificRecord;

public interface ArchiverStrategy {
	/**
	 * Reads records of type {@code T} using the provided Avro {@link Schema}.
	 * Each record is passed to the specified {@code recordHandler} function.
	 * <p>
	 * The reading process continues until there are no more records or the
	 * {@code recordHandler} returns {@code false}, whichever comes first.
	 *
	 * @param <T> the type of the records, extending {@link SpecificRecord}
	 * @param schema the Avro {@link Schema} to use for decoding records
	 * @param recordHandler a function that processes each decoded record;
	 *                      returns {@code true} to continue reading,
	 *                      or {@code false} to stop early
	 * @throws IOException if an error occurs while reading records
	 */
    public abstract <T extends SpecificRecord> void read(
		Schema schema,
		Function<T, Boolean> recordHandler 
	) throws IOException; 
	/**
	 * Writes records of type {@code T} using the provided Avro {@link Schema}.
	 * <p>
	 * Records are obtained from the {@code recordSupplier} and written using the
	 * specified schema. The write operation may serialize the records to a file,
	 * stream, or another output target depending on the implementation.
	 *
	 * @param <T> the type of records to write, extending {@link SpecificRecord}
	 * @param schema the Avro {@link Schema} used to encode the records
	 * @param recordSupplier a supplier that provides the list of records to be written
	 * @throws IOException if an error occurs during the write process
	 */
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