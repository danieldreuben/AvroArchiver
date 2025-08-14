package com.ross.serializer.stategy;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;

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
import java.util.zip.GZIPOutputStream;

/**
 * Provides a generic, lightweight helper for creating, updating, and querying
 * a Lucene index to enable fast lookup of Avro file locations or other resources
 * based on a searchable key.
 */
public class LuceneIndexHelper implements Closeable, IndexerPlugin {
    protected final Path indexPath;
    protected Directory directory;
    protected IndexWriter indexWriter;
    protected IndexReader reader;

    public LuceneIndexHelper(Path indexPath) throws IOException {
        this.indexPath = indexPath;
        //this.directory = FSDirectory.open(indexPath);
        open(indexPath);
    }

    @Override
    public void open(Path indexPath) throws IOException {
        if (indexWriter != null && indexWriter.isOpen()) {
            System.out.println("IndexWriter already open: " + indexPath);
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

        System.out.println("-->sidecar: Lucene index at: " + indexPath);
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

    public static class MatchResult {
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
    }

    @Override
    public List<MatchResult> findLocationsForIndex(String indexPattern) throws Exception {
        if (reader != null) {
            reader.close();
            reader = null;
        }

        if (!DirectoryReader.indexExists(directory)) {
            System.out.println("No index exists at " + indexPath);
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

        TopDocs results = searcher.search(query, 100);

        List<MatchResult> matches = new ArrayList<>();
        for (ScoreDoc sd : results.scoreDocs) {
            Document doc = searcher.doc(sd.doc);
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
        System.out.println("Deleted documents matching: " + normalizedPattern);
    }

    @Override
    public void archiveIndex() throws IOException {
        archiveDirectoryToTarGz(indexPath.toString());
    }

    public static void archiveDirectoryToTarGz(String dirPath) throws IOException {
        Path sourceDir = Paths.get(dirPath);
        if (!Files.isDirectory(sourceDir)) {
            throw new IllegalArgumentException("Path is not a directory: " + dirPath);
        }

        String outputFileName = sourceDir.getFileName().toString() + ".tar.gz";
        Path outputPath = sourceDir.getParent() != null
                ? sourceDir.getParent().resolve(outputFileName)
                : Paths.get(outputFileName);

        try (FileOutputStream fos = new FileOutputStream(outputPath.toFile());
             GZIPOutputStream gos = new GZIPOutputStream(fos);
             TarArchiveOutputStream taos = new TarArchiveOutputStream(gos)) {

            taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

            Files.walk(sourceDir).forEach(path -> {
                try {
                    String relativePath = sourceDir.relativize(path).toString();
                    if (Files.isDirectory(path)) {
                        return;
                    }
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

        System.out.println("Archived " + sourceDir + " to " + outputPath);
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
