package com.ross.excel.serializer.archiver;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;


public class AvroFileSystemStrategy<T extends SpecificRecord> extends AvroStreamingStrategy<T> {

    public AvroFileSystemStrategy(String job) {
		//this.jobParams = ArchiveJobParams2.getInstance(job);		
        super(job);
    } 
    
	/*public AvroFileSystemStrategy() {
        super();
	}   */ 

    @Override
    public <T extends SpecificRecord> void read(
        Schema schema,
        Function<T, Boolean> recordHandler
    ) throws IOException {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);

        try (SeekableInput input = new SeekableFileInput(file)) {
            super.read(schema, input, recordHandler); 
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public <T extends SpecificRecord> void readAll(
        Schema schema,
        Consumer<List<T>> recordHandler
    ) throws IOException {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);

        try (SeekableInput input = new SeekableFileInput(file)) {
            super.readAll(schema, input, recordHandler); 
        } catch (Exception e) {
            e.printStackTrace();
            throw e; 
        }
    }

    @Override
    public <T extends SpecificRecord> void readBatched(
        Schema schema,
        Function<List<T>, Boolean> recordHandler
    ) throws IOException {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);

        try (SeekableInput input = new SeekableFileInput(file)) {
            super.readBatched(schema, input, recordHandler); 
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public <T extends SpecificRecord> void write(
        Schema schema,
        Supplier<List<T>> recordSupplier
    ) throws IOException {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);

        try {
            SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(schema);
            DataFileWriter<T> dataFileWriter = new DataFileWriter<>(writer);

            if (file.exists()) {
                dataFileWriter.appendTo(file); 
            } else {
                dataFileWriter.create(schema, file);  
            }

            List<T> batch;
            while (!(batch = recordSupplier.get()).isEmpty()) {
                for (T record : batch) {
                    dataFileWriter.append(record);
                    System.out.print(".");
                }
            }

            dataFileWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public <T extends SpecificRecord> void findMatchingRecords(
        Class<T> clazz,
        Predicate<T> recordFilter,
        Function<T, Boolean> onMatch
    ) throws Exception {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);

        try (SeekableInput input = new SeekableFileInput(file)) {
            super.findMatchingRecords(clazz, input, recordFilter, onMatch);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
