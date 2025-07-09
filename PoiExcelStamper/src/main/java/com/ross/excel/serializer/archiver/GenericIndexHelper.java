package com.ross.excel.serializer.archiver;

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