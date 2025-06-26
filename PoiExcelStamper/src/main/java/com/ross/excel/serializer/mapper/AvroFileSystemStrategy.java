package com.ross.excel.serializer.mapper;

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

public class AvroFileSystemStrategy implements ArchiverStrategy {

    private String baseDir = null;
	private AvroArchiveCmd archiveCommand;
	private String avroArchiveFilename; 

    public AvroFileSystemStrategy(AvroArchiveCmd cmd) {

		this.archiveCommand = cmd;		
        /**this.baseDir = archiveCommand.getBaseDir() != null ? 
			cmd.getBaseDir() : System.getProperty("user.dir");	*/
		this.baseDir = System.getProperty("user.dir");		
		avroArchiveFilename = ArchiveNameResolver.
			resolveAvroArchiveFileName(cmd.getJobName(), cmd.getArchiveSchedule());
    } 

    @Override
    public void archive() throws IOException {
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
			for (SpecificRecord record : archiveCommand.getRecords()) {
				writer.append(record);
			}
			writer.flush();
		}
	}

	@Override
    public List<SpecificRecord> read() throws IOException {
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
        return records;
    }
}
