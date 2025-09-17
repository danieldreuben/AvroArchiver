package com.ross.serializer.stategy;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

/**
 * Provides a generic, lightweight helper for creating, updating, and querying
 * a Lucene index to enable fast lookup of Avro file locations or other resources
 * based on a searchable key.
 */
 
public class LuceneIndexHelper<T> implements Closeable, IndexerPlugin<T> {
	private static final Logger log = LoggerFactory.getLogger(LuceneIndexHelper.class);

    protected final Path indexPath;
    protected Directory directory;
    protected IndexWriter indexWriter;
    protected IndexReader reader;
    private Function<T, String> keyExtractor;

    public LuceneIndexHelper(Path indexPath) throws IOException {
        this.indexPath = indexPath;
        //this.directory = FSDirectory.open(indexPath);
        open(indexPath);
    }

    @Override
    public void setKeyExtractor(Function<T, ?> keyExtractor) {
        this.keyExtractor = t -> {
            Object key = keyExtractor.apply(t);
            return key != null ? key.toString() : null;
        };
       log.debug("key-extractor set: " + this.keyExtractor);
    }


    @Override
    public void index(T record, ArchiveJobParams params) throws Exception {
        if (keyExtractor == null) {
            throw new IllegalStateException("Key extractor is not set");
        }
        String key = keyExtractor.apply(record);
        indexRecord(key, params.getNaming());
        //System.out.println("Indexed "+ params.getNaming() +" with key: " + key);
        //System.out.print('i');
    }

    //@Override
    protected void open(Path indexPath) throws IOException {
        if (indexWriter != null && indexWriter.isOpen()) {
            log.debug("IndexWriter already open: " + indexPath);
            return;
        }

        boolean indexExists;
        try (Directory tempDir = FSDirectory.open(indexPath)) {
            indexExists = DirectoryReader.indexExists(tempDir);
        }

        if (directory == null) {
            directory = FSDirectory.open(indexPath);
        }

        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setOpenMode(indexExists
                ? IndexWriterConfig.OpenMode.APPEND
                : IndexWriterConfig.OpenMode.CREATE);

        log.debug("-->sidecar: Lucene index at: " + indexPath);
        indexWriter = new IndexWriter(directory, config);
    }

    @Override
    public void indexRecord(String indexKey, String location) throws IOException {
        if (indexWriter == null || !indexWriter.isOpen()) {
            throw new IllegalStateException("IndexWriter is not open. Call open() first.");
        }

        String normalizedKey = indexKey.toUpperCase();

        Document doc = new Document();
        doc.add(new StringField("index", normalizedKey, Field.Store.YES));
        doc.add(new StringField("location", location, Field.Store.YES));
        doc.add(new LongPoint("timestamp", Instant.now().toEpochMilli()));
        doc.add(new StoredField("timestamp", Instant.now().toString()));

        indexWriter.addDocument(doc);
    }

    @Override
    public void indexAndCommit(String indexKey, String location) throws IOException {
        indexRecord(indexKey, location);
        indexWriter.commit();
    }

    /*public static class MatchResult {
        private final String index;
        private final String location;

        public MatchResult(String index, String location) {
            this.index = index;
            this.location = location;
        }

        public String getIndex() {
            return index;
        }

        public String getLocation() {
            return location;
        }

        @Override
        public String toString() {
            return "MatchResult{index='" + index + "', location='" + location + "'}";
        }
    }*/

    @Override
    public List<MatchResult> findLocationsForIndex(String indexPattern) throws Exception {
        if (reader != null) {
            reader.close();
            reader = null;
        }

        if (!DirectoryReader.indexExists(directory)) {
            log.info("No index exists at " + indexPath);
            return List.of();
        }

        if (indexWriter != null && indexWriter.isOpen()) {
            reader = DirectoryReader.open(indexWriter);
        } else {
            reader = DirectoryReader.open(directory);
        }

        IndexSearcher searcher = new IndexSearcher(reader);

        String normalizedPattern = indexPattern.toUpperCase();
        Query query;

        if (normalizedPattern.contains("*") || normalizedPattern.contains("?")) {
            query = new WildcardQuery(new Term("index", normalizedPattern));
        } else {
            query = new TermQuery(new Term("index", normalizedPattern));
        }

        TopDocs results = searcher.search(query, 500);

        List<MatchResult> matches = new ArrayList<>();
        StoredFields storedFields = reader.storedFields();

        for (ScoreDoc sd : results.scoreDocs) {
            Document doc = storedFields.document(sd.doc);
            String location = doc.get("location");
            String index = doc.get("index");
            if (location != null && index != null) {
                matches.add(new MatchResult(index, location));
            }
        }

        return matches;
    }


    @Override
    public void deleteKeys(String indexPattern) throws IOException {
        if (indexWriter == null || !indexWriter.isOpen()) {
            throw new IllegalStateException("IndexWriter is not open. Call open() first.");
        }

        String normalizedPattern = indexPattern.toUpperCase();
        Query query = new WildcardQuery(new Term("index", normalizedPattern));
        indexWriter.deleteDocuments(query);
        indexWriter.commit();
        log.debug("Deleted documents matching: " + normalizedPattern);
    }

    @Override
    public void archiveIndex(ArchiveJobParams jobParams) throws IOException {
        log.debug("archive " + jobParams.getStorage().getFile().getArchiveDir());
        //System.out.println("base " + jobParams.getStorage().getFile().getBaseDir());   
        log.debug("indexer " + jobParams.getIndexer().getName());   
        archiveDirectoryToTarGz(indexPath.toString(), jobParams.getIndexer().getName());
    }

    @Override    
    public String extractKey(T record) {
        return keyExtractor.apply(record);
    }

    public static void archiveDirectoryToTarGz(String dirPath, String destPath) throws IOException {
    Path sourceDir = Paths.get(dirPath);
    if (!Files.isDirectory(sourceDir)) {
        throw new IllegalArgumentException("Path is not a directory: " + dirPath);
    }

    Path outputPath = Paths.get(destPath);
    if (Files.isDirectory(outputPath)) {
        // If destination is a directory, place the tar.gz inside it
        String outputFileName = sourceDir.getFileName().toString() + ".tar.gz";
        outputPath = outputPath.resolve(outputFileName);
    }

    try (FileOutputStream fos = new FileOutputStream(outputPath.toFile());
         GZIPOutputStream gos = new GZIPOutputStream(fos);
         TarArchiveOutputStream taos = new TarArchiveOutputStream(gos)) {

        taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

        Files.walk(sourceDir).forEach(path -> {
            try {
                if (Files.isDirectory(path)) {
                    return;
                }
                String relativePath = sourceDir.relativize(path).toString();
                TarArchiveEntry entry = new TarArchiveEntry(path.toFile(), relativePath);
                taos.putArchiveEntry(entry);
                Files.copy(path, taos);
                taos.closeArchiveEntry();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        taos.finish();
    }

    log.debug("Archived " + sourceDir + " to " + outputPath);
}


    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
            reader = null;
        }
        if (indexWriter != null && indexWriter.isOpen()) {
            indexWriter.close();
            indexWriter = null;
        }
    }
}
