
package com.ross.serializer.stategy;

import com.azure.storage.blob.*;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.*;
import java.util.List;
import java.util.function.*;

public class AvroBlobStorageStrategy<T extends SpecificRecord> extends AvroStreamingStrategy<T> {

    private final BlobClient blobClient;

    public AvroBlobStorageStrategy(String job) {
        this.jobParams = ArchiveJobParams.getInstance(job);
        this.blobClient = new BlobClientBuilder()
                .endpoint(jobParams.getStorage().getBlob().getEndpoint())
                .containerName(jobParams.getStorage().getBlob().getContainer())
                .blobName(jobParams.getNaming())
                .buildClient();
    }

    @Override
    public <T extends SpecificRecord> void read(
        Schema schema,
        Function<T, Boolean> recordHandler
    ) throws IOException {
        try (SeekableInput input = toSeekableInput()) {
            super.read(schema, input, recordHandler);
        }
    }

    @Override
    public <T extends SpecificRecord> void readAll(
        Schema schema,
        Consumer<List<T>> recordHandler
    ) throws IOException {
        try (SeekableInput input = toSeekableInput()) {
            super.readAll(schema, input, recordHandler);
        }
    }

    @Override
    public <T extends SpecificRecord> void readBatched(
        Schema schema,
        Function<List<T>, Boolean> recordHandler
    ) throws IOException {
        try (SeekableInput input = toSeekableInput()) {
            super.readBatched(schema, input, recordHandler);
        }
    }

    @Override
    public <T extends SpecificRecord> void write(
        Schema schema,
        Supplier<List<T>> recordSupplier
    ) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(schema);
        DataFileWriter<T> dataFileWriter = new DataFileWriter<>(writer);

        dataFileWriter.create(schema, outputStream);

        List<T> batch;
        while (!(batch = recordSupplier.get()).isEmpty()) {
            for (T record : batch) {
                dataFileWriter.append(record);
                System.out.print(".");
            }
        }

        dataFileWriter.close();

        byte[] data = outputStream.toByteArray();
        blobClient.upload(new ByteArrayInputStream(data), data.length, true);
    }

    @Override
    public <T extends SpecificRecord> void findMatchingRecords(
        Class<T> clazz,
        Predicate<T> recordFilter,
        Function<T, Boolean> onMatch
    ) throws Exception {
        try (SeekableInput input = toSeekableInput()) {
            super.findMatchingRecords(clazz, input, recordFilter, onMatch);
        }
    }

    private SeekableInput toSeekableInput() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        blobClient.download(output);
        return new SeekableByteArrayInput(output.toByteArray());
    }
}
