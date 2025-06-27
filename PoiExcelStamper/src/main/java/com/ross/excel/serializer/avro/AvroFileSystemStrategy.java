package com.ross.excel.serializer.avro;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import com.ross.excel.serializer.archiver.ArchiverStrategy;
import com.ross.excel.serializer.archiver.ArchiveNameResolver;

public class AvroFileSystemStrategy<T extends SpecificRecord> implements ArchiverStrategy {

    private String baseDir = null;
	private AvroArchiveCmd<T> archiveCommand;
	private String avroArchiveFilename; 

    public AvroFileSystemStrategy(AvroArchiveCmd<T> cmd) {

		this.archiveCommand = cmd;		
        /**this.baseDir = archiveCommand.getBaseDir() != null ? 
			cmd.getBaseDir() : System.getProperty("user.dir");	*/
		this.baseDir = System.getProperty("user.dir");		
		avroArchiveFilename = ArchiveNameResolver.
			resolveAvroArchiveFileName(cmd.getJobName(), cmd.getArchiveSchedule());
    } 

    @Override
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
			/*for (SpecificRecord record : archiveCommand.getRecords()) {
				writer.append(record);
			}*/
			writer.flush();
		}
	}

	@Override
    public List<SpecificRecord> deserialize() throws IOException {
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
            }
        }
		//archiveCommand.setRecords(records);
        return records;
    }
}
