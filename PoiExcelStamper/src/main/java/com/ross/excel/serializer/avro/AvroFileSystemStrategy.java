package com.ross.excel.serializer.avro;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import com.ross.excel.serializer.archiver.ArchiverStrategy;
import com.ross.excel.serializer.archiver.ArchiveNameResolver;

public class AvroFileSystemStrategy<T extends SpecificRecord> /* implements ArchiverStrategy */ {

    private String baseDir = null;
	private ArchiveCommand<T> archiveCommand;
	private String avroArchiveFilename; 

    public AvroFileSystemStrategy(ArchiveCommand<T> cmd) {

		this.archiveCommand = cmd;		
        /**this.baseDir = archiveCommand.getBaseDir() != null ? 
			cmd.getBaseDir() : System.getProperty("user.dir");	*/
		this.baseDir = System.getProperty("user.dir");		
		avroArchiveFilename = ArchiveNameResolver.
			resolveAvroArchiveFileName(cmd.getJobName(), cmd.getArchiveSchedule());
    } 

	 public AvroFileSystemStrategy() {
	 }


	public <T extends SpecificRecord> void deserialize3(Schema schema,
			File avroFile, int batchSize, Consumer<List<T>> recordHandler
	) throws IOException {
		List<T> records = new ArrayList<>();
		SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);

		try (DataFileReader<T> dataFileReader = new DataFileReader<>(avroFile, reader)) {
			while (dataFileReader.hasNext()) {
				T record = dataFileReader.next();
				records.add(record);
				if (records.size() == batchSize) { 
					recordHandler.accept(records);
					records.clear();
				}
			}
			if (!records.isEmpty()) {
				recordHandler.accept(records);
			}
		}		
	}

	public <T extends SpecificRecord> void serializeBatched(Schema schema,
			File avroFile, int batchSize, Supplier<List<T>> recordSupplier
		) throws IOException {
		
		SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(schema);

		try (DataFileWriter<T> dataFileWriter = new DataFileWriter<>(writer)) {
			dataFileWriter.create(schema, avroFile);

			List<T> batch;
			while (!(batch = recordSupplier.get()).isEmpty()) {
				for (T record : batch) {
					dataFileWriter.append(record);
				}
			}
		}
	}

    public static <T extends SpecificRecord> List<T> find(
            Schema schema,
            File avroFile,
            Predicate<T> filter
    ) throws IOException {
        List<T> matchingRecords = new ArrayList<>();
        SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);

        try (DataFileReader<T> dataFileReader = new DataFileReader<>(avroFile, reader)) {
            while (dataFileReader.hasNext()) {
                T record = dataFileReader.next();
                if (filter.test(record)) {
                    matchingRecords.add(record);
                }
            }
        }
        return matchingRecords;
    }	

	public static <T extends SpecificRecord> void findWithHandler(
            Schema schema,
            File avroFile,
            Consumer<T> recordHandler
    ) throws IOException {

        SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);

        try (DataFileReader<T> dataFileReader = new DataFileReader<>(avroFile, reader)) {
            while (dataFileReader.hasNext()) {
                T record = dataFileReader.next();
                recordHandler.accept(record); 
            }
        }
    }
	
}




    /*@Override
    public void serialize() throws IOException {
		File outFile = new File(baseDir, avroArchiveFilename);
		outFile.getParentFile().mkdirs();
		boolean append = outFile.exists();

		try (FileOutputStream fos = new FileOutputStream(outFile, append);
			DataFileWriter<SpecificRecord> writer = new DataFileWriter<>(new SpecificDatumWriter<>())) {

			if (!append) {
				writer.create(archiveCommand.getSchema(), fos);
			} else {
				try (DataFileReader<SpecificRecord> reader = 
					new DataFileReader<>(outFile, new SpecificDatumReader<>())) {
						
						writer.appendTo(outFile);
				}
			}
			List<T> records; 
			while (!(records = archiveCommand.getRecords()).isEmpty()) {
				for (SpecificRecord record : records) {
					writer.append(record);
				}
			}

			writer.flush();
		}
	}*/

	/*@Override
    public void deserialize() throws IOException {
        List<SpecificRecord> records = new ArrayList<>();
		File avroFile = new File(baseDir, avroArchiveFilename);
		SpecificDatumReader<? extends SpecificRecord> reader = 
			new SpecificDatumReader<>(archiveCommand.getSchema());
        System.out.println(">>>" + avroFile);

		try (DataFileReader<? extends SpecificRecord> dataFileReader = 
			new DataFileReader<>(avroFile, reader)) {

            while (dataFileReader.hasNext()) {
                SpecificRecord record = dataFileReader.next();
                records.add(record);
				if (records.size() == archiveCommand.getReadBatchSize()) {
					//archiveCommand.setRecords(records);
					records.clear();
				}
            }
        }
    }*/

