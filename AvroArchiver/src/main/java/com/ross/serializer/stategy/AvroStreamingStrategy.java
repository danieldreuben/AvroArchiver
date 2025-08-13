package com.ross.serializer.stategy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;


public abstract class AvroStreamingStrategy<T extends SpecificRecord>  implements ArchiverStrategy  {

	protected ArchiveJobParams jobParams;
    //GenericIndexHelper indexHelper;

    public AvroStreamingStrategy(String job) {
		this.jobParams = ArchiveJobParams.getInstance(job);	
    } 

	public AvroStreamingStrategy() {
	}

	public ArchiveJobParams getJobParams() {
		return jobParams;
	}

	protected <T extends SpecificRecord> void read(
    Schema schema,
    SeekableInput input,
    Function<T, Boolean> recordHandler
	) throws IOException {
		SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);

		try (DataFileReader<T> dataFileReader = new DataFileReader<>(input, reader)) {
			while (dataFileReader.hasNext()) {
				T record = dataFileReader.next();
				boolean shouldContinue = recordHandler.apply(record);
				if (!shouldContinue) {
					System.out.println("Terminated deserialization handler on response");
					break;
				}
			}
		}
	}

	public <T extends SpecificRecord> void readBatched(
		Schema schema,
		SeekableInput input,
		Function<List<T>, Boolean> recordHandler
	) throws IOException {
		List<T> records = new ArrayList<>();
		SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);

		try (DataFileReader<T> dataFileReader = new DataFileReader<>(input, reader)) {
			while (dataFileReader.hasNext()) {
				T record = dataFileReader.next();
				records.add(record);

				if (records.size() >= jobParams.getJob().getBatchRead() || !dataFileReader.hasNext()) {
					boolean shouldContinue = recordHandler.apply(records);
					records.clear();
					if (!shouldContinue) {
						System.out.println("Terminated deserialization handler on response");
						break;
					}
				}
			}
		}
	}

/* 	public <T extends SpecificRecord> void write(
		Schema schema,
		OutputStream outputStream,
		Supplier<List<T>> recordSupplier
	) throws IOException {

		SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(schema);

		try (DataFileWriter<T> dataFileWriter = new DataFileWriter<>(writer)) {
			if (jobParams.getJob().getDeflate() > 0) {
				dataFileWriter.setCodec(CodecFactory.deflateCodec(jobParams.getJob().getDeflate()));
			}

			dataFileWriter.create(schema, outputStream);

			List<T> batch;
			while (!(batch = recordSupplier.get()).isEmpty()) {
				for (T record : batch) {
					dataFileWriter.append(record);
                    System.out.print("$");					
				}
			}
		}
	} */

	public <T extends SpecificRecord> void write(
		Schema schema,
		Supplier<List<T>> recordSupplier,
		File file		
	) throws IOException {
		SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(schema);

		try (DataFileWriter<T> dataFileWriter = new DataFileWriter<>(writer)) {
			if (jobParams.getJob().getDeflate() > 0) {
				dataFileWriter.setCodec(CodecFactory.deflateCodec(jobParams.getJob().getDeflate()));
			}

			if (file.exists()) {
				dataFileWriter.appendTo(file);
			} else {
				dataFileWriter.create(schema, file);
			}
			List<T> batch;
			while (!(batch = recordSupplier.get()).isEmpty()) {
				for (T record : batch) {
					dataFileWriter.append(record);
					System.out.print("$");
				}
			}
		}
	}

	public <T extends SpecificRecord> void readAll(
		Schema schema,
		SeekableInput input,
		Consumer<List<T>> recordHandler
	) throws IOException {
		List<T> records = new ArrayList<>();
		SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);

		try (DataFileReader<T> dataFileReader = new DataFileReader<>(input, reader)) {
			while (dataFileReader.hasNext()) {
				T record = dataFileReader.next();
				records.add(record);
				if (records.size() >= jobParams.getJob().getBatchRead() || !dataFileReader.hasNext()) {
					recordHandler.accept(records);
					records.clear();
				}
			}
		}
	}

	public <T extends SpecificRecord> Optional<T> find(
		Schema schema,
		Predicate<T> recordFilter
	) throws IOException {
		SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
		String fullPath = jobParams.getNaming();

		try (DataFileReader<T> dataFileReader = new DataFileReader<>(new File(fullPath), reader)) {
			while (dataFileReader.hasNext()) {
				T record = dataFileReader.next();
				if (recordFilter.test(record)) {
					return Optional.of(record);
				}
			}
		}
		return Optional.empty();
	}


	public <T extends SpecificRecord> void findMatchingRecords(
		Class<T> clazz,
		SeekableInput input,
		Predicate<T> recordFilter,
		Function<T, Boolean> onMatch
	) throws Exception {
		T instance = clazz.getDeclaredConstructor().newInstance();
		Schema schema = instance.getSchema();

		SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);

		try (DataFileReader<T> dataFileReader = new DataFileReader<>(input, reader)) {
			while (dataFileReader.hasNext()) {
				T record = dataFileReader.next();
				if (recordFilter.test(record)) {
					boolean shouldContinue = onMatch.apply(record);
					if (!shouldContinue) break;
				}
			}
		}
	}
}

