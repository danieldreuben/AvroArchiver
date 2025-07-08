package com.ross.excel.serializer.archiver;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
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
        super(job);
    } 

    @Override
    public <T extends SpecificRecord> void read(
        Schema schema,
        Function<T, Boolean> recordHandler
    ) throws IOException {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);
        //System.out.println(fullPath);

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
        //System.out.println(fullPath);

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
        //System.out.println(fullPath);

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
        try {
            String fullPath = jobParams.getNaming();
            File file = new File(fullPath);
            //System.out.println(fullPath);
            super.write2(schema, file, recordSupplier);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    /*@Override
    public <T extends SpecificRecord> void write(
        Schema schema,
        Supplier<List<T>> recordSupplier
    ) throws IOException {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);
        System.out.println(fullPath);

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
                super.writeIndex(batch);
            }        

            dataFileWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }*/

    @Override
    public <T extends SpecificRecord> void findMatchingRecords(
        Class<T> clazz,
        Predicate<T> recordFilter,
        Function<T, Boolean> onMatch
    ) throws Exception {
        String fullPath = jobParams.getNaming();
        File file = new File(fullPath);
        //System.out.println(fullPath);

        try (SeekableInput input = new SeekableFileInput(file)) {
            super.findMatchingRecords(clazz, input, recordFilter, onMatch);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

   /* public Optional<List<String>> find(String id) {
        try {
            return Optional.ofNullable(indexHelper.findLocationsForIndex(id));
        } catch (Exception e) {
            System.err.println("Failed to find locations for index [" + id + "]: " + e.getMessage());
            e.printStackTrace();
            return Optional.empty();
        }
    }*/
    public Optional<List<GenericIndexHelper.MatchResult>> find(String id) {
        try {
            List<GenericIndexHelper.MatchResult> results = indexHelper.findLocationsForIndex(id);
            return results.isEmpty() ? Optional.empty() : Optional.of(results);
        } catch (Exception e) {
            System.err.println("Failed to find locations for index [" + id + "]: " + e.getMessage());
            e.printStackTrace();
            return Optional.empty();
        }
    }

}
