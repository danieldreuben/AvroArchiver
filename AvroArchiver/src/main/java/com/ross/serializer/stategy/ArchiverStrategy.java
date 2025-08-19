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
import org.apache.avro.file.SeekableInput;
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

	/**
	 * Writes a batch of Avro records to the specified file using the provided schema.
	 * <p>
	 * This method serializes a collection of Avro {@link SpecificRecord} instances into
	 * the Avro data file format. The records are obtained lazily from the given
	 * {@code recordSupplier}, allowing flexible record generation or retrieval from
	 * external sources (e.g., databases, streams) at runtime.
	 * </p>
	 *
	 * <p>
	 * Typical use cases include:
	 * <ul>
	 *   <li>Archiving a batch of domain events (e.g., orders) into an Avro file.</li>
	 *   <li>Persisting records for later query or indexing.</li>
	 *   <li>Exporting Avro data for downstream ingestion pipelines.</li>
	 * </ul>
	 * </p>
	 *
	 * @param <T>             the concrete type of the Avro {@link SpecificRecord}
	 *                        being written (e.g., {@code OrderAvro}).
	 * @param schema          the Avro {@link Schema} that defines the structure of the records;
	 *                        must match the {@link SpecificRecord} type.
	 * @param recordSupplier  a {@link Supplier} that returns the list of records to write.
	 *                        The supplier is invoked once during this method, allowing for
	 *                        lazy record creation.
	 * @param file            the {@link File} to which the Avro data will be written. If the
	 *                        file exists, its contents will be overwritten.
	 *
	 * @throws IOException    if an error occurs during writing, such as file I/O issues
	 *                        or serialization failures.
	 *
	 * @see org.apache.avro.file.DataFileWriter
	 * @see org.apache.avro.specific.SpecificDatumWriter
	 */
	public <T extends SpecificRecord> void write(
		Schema schema,
		Supplier<List<T>> recordSupplier,
		File file
	) throws Exception;

	/**
	 * Finds the first Avro record matching the given filter predicate.
	 * <p>
	 * This method reads records of the specified {@link SpecificRecord} type
	 * from one or more underlying Avro data sources and applies the provided
	 * {@code recordFilter} to each record until a match is found.
	 * If no record satisfies the predicate, an empty {@link Optional} is returned.
	 * </p>
	 *
	 * <p>
	 * Typical use cases include:
	 * <ul>
	 *   <li>Searching for a specific entity (e.g., an order by {@code orderId}) within an Avro dataset.</li>
	 *   <li>Locating a single record to drive downstream processing or validation.</li>
	 *   <li>Filtering archived Avro data for quick lookups without full dataset scanning.</li>
	 * </ul>
	 * </p>
	 *
	 * @param <T>           the concrete type of the Avro {@link SpecificRecord}
	 *                      being searched (e.g., {@code OrderAvro}).
	 * @param schema        the Avro {@link Schema} describing the records in the dataset;
	 *                      must match the {@link SpecificRecord} type.
	 * @param recordFilter  a {@link Predicate} applied to each record to determine
	 *                      if it matches the search criteria. The first match found
	 *                      is returned.
	 *
	 * @return an {@link Optional} containing the first matching record, or
	 *         {@link Optional#empty()} if no match is found.
	 *
	 * @throws IOException  if an error occurs during reading, such as I/O failures
	 *                      or deserialization issues.
	 *
	 * @see java.util.function.Predicate
	 * @see java.util.Optional
	 * @see org.apache.avro.file.DataFileReader
	 * @see org.apache.avro.specific.SpecificDatumReader
	 */
	public abstract <T extends SpecificRecord> Optional<T> find(
		Schema schema,
		Predicate<T> recordFilter
	) throws IOException;


	/**
	 * Reads all Avro records of the specified type and processes them in batches
	 * using the provided record handler.
	 * <p>
	 * This method deserializes records from one or more Avro data sources
	 * according to the provided {@link Schema}, grouping them into lists that are
	 * passed to the {@code recordHandler} for processing. The method is designed
	 * for batch-oriented operations, allowing efficient handling of large datasets
	 * without loading the entire dataset into memory at once.
	 * </p>
	 *
	 * <p>
	 * Typical use cases include:
	 * <ul>
	 *   <li>Processing archived Avro data in chunks for analytics or reporting.</li>
	 *   <li>Bulk exporting records to another system or format.</li>
	 *   <li>Feeding records into downstream pipelines (e.g., Kafka, Azure Event Hub).</li>
	 * </ul>
	 * </p>
	 *
	 * @param <T>            the concrete type of the Avro {@link SpecificRecord}
	 *                       being read (e.g., {@code OrderAvro}).
	 * @param schema         the Avro {@link Schema} describing the structure of the records;
	 *                       must match the {@link SpecificRecord} type.
	 * @param recordHandler  a {@link Consumer} that accepts each batch of records read.
	 *                       The size of each list depends on the underlying implementation.
	 *
	 * @throws IOException   if an error occurs during reading, such as I/O failures
	 *                       or deserialization issues.
	 *
	 * @see java.util.function.Consumer
	 * @see org.apache.avro.file.DataFileReader
	 * @see org.apache.avro.specific.SpecificDatumReader
	 */
	public abstract <T extends SpecificRecord> void readAll(
		Schema schema,
		Consumer<List<T>> recordHandler
	) throws IOException;


	/**
	 * Reads Avro records of the specified type in batches and processes each batch
	 * using the provided record handler function, allowing the caller to control
	 * whether reading should continue.
	 * <p>
	 * This method deserializes records from one or more Avro data sources according
	 * to the provided {@link Schema}, grouping them into lists and passing each list
	 * to the {@code recordHandler}. The handler returns a {@code boolean} indicating
	 * whether to continue reading:
	 * <ul>
	 *   <li>{@code true} – continue processing subsequent batches</li>
	 *   <li>{@code false} – stop reading immediately</li>
	 * </ul>
	 * This makes the method useful for early termination scenarios (e.g., finding the
	 * first N matching records or stopping when a certain condition is met).
	 * </p>
	 *
	 * <p>
	 * Typical use cases include:
	 * <ul>
	 *   <li>Scanning Avro files until a specific record or threshold is found.</li>
	 *   <li>Streaming large datasets in manageable chunks.</li>
	 *   <li>Partial dataset reads for performance optimization.</li>
	 * </ul>
	 * </p>
	 *
	 * @param <T>            the concrete type of the Avro {@link SpecificRecord}
	 *                       being read (e.g., {@code OrderAvro}).
	 * @param schema         the Avro {@link Schema} describing the records to read;
	 *                       must match the {@link SpecificRecord} type.
	 * @param recordHandler  a {@link Function} that accepts a batch of records and returns
	 *                       {@code true} to continue reading or {@code false} to stop.
	 *
	 * @throws IOException   if an error occurs during reading, such as I/O failures
	 *                       or deserialization issues.
	 *
	 * @see java.util.function.Function
	 * @see org.apache.avro.file.DataFileReader
	 * @see org.apache.avro.specific.SpecificDatumReader
	 */
	public abstract <T extends SpecificRecord> void readBatched(
		Schema schema,
		Function<List<T>, Boolean> recordHandler 
	) throws IOException;


	public abstract <T extends SpecificRecord> void findMatchingRecords(
		Class<T> clazz,
		Predicate<T> recordFilter,
		Function<T, Boolean> onMatch
	) throws Exception;

	public <T extends SpecificRecord> long fastCountRecords(
		Class<T> clazz,
		SeekableInput input
	) throws IOException;

	public abstract boolean put(String name);
	public abstract boolean get(String name);
	public abstract List<String> getNames(String ref);	
	public ArchiveJobParams getJobParams();
}