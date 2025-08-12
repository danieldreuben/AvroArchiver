package com.ross.serializer.stategy;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Provides a generic, lightweight helper for creating, updating, and querying
 * a Lucene index to enable fast lookup of Avro file locations or other resources
 * based on a searchable key.
 * <p>
 * This class is intended to act as a sidecar indexing utility for serialized
 * data files (e.g., Avro archives), allowing:
 * <ul>
 *   <li>Efficient creation and maintenance of an on-disk Lucene index.</li>
 *   <li>Indexing of key-to-location mappings for quick retrieval.</li>
 *   <li>Support for both exact and wildcard queries over indexed keys.</li>
 *   <li>Timestamp storage for each indexed entry for auditing or recency checks.</li>
 * </ul>
 * </p>
 *
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * Path indexPath = Paths.get("/tmp/order-index");
 * try (GenericIndexHelper indexHelper = new GenericIndexHelper(indexPath)) {
 *     indexHelper.open();
 *     indexHelper.indexAndCommit("ORDER-12345", "/data/orders/order-12345.avro");
 *
 *     List<GenericIndexHelper.MatchResult> results =
 *         indexHelper.findLocationsForIndex("ORDER-12345");
 *     results.forEach(System.out::println);
 * }
 * }</pre>
 *
 * <p>
 * This helper uses a {@link StandardAnalyzer} for text analysis and supports
 * wildcard searches via Lucene’s {@link WildcardQuery}. It is designed to be
 * opened once, reused for multiple operations, and closed when no longer needed.
 * </p>
 *
 * <h3>Thread Safety:</h3>
 * This class is <strong>not</strong> thread-safe. If used concurrently from multiple
 * threads, synchronization must be handled externally.
 * </h3>
 *
 * @author 
 * @see org.apache.lucene.index.IndexWriter
 * @see org.apache.lucene.index.DirectoryReader
 * @see org.apache.lucene.search.IndexSearcher
 */

public class GenericIndexHelper implements Closeable {
    private final Path indexPath;
    private Directory directory;
    private IndexWriter indexWriter;
    private IndexReader reader;

    public GenericIndexHelper(Path indexPath) throws IOException {
        this.indexPath = indexPath;
        this.directory = FSDirectory.open(indexPath);
    }

    public void open() throws IOException {
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

        System.out.println("-->sidecar: Lucerne index at: " + indexPath);
        indexWriter = new IndexWriter(directory, config);
    }

    public void indexAndCommit(String indexKey, String location) throws IOException {
        indexRecord(indexKey, location);
        indexWriter.commit();
    }

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