package com.ross.serializer.stategy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Generic interface for an indexer plugin that supports
 * indexing, querying, deleting, and archiving of an index.
 */
public interface IndexerPlugin {

    /**
     * Opens or initializes the index at the given path.
     *
     * @param indexPath Path to the index directory
     * @throws IOException if the index cannot be opened
     */
    void open(Path indexPath) throws IOException;

    /**
     * Indexes a single record in the index.
     *
     * @param indexKey The key to index
     * @param location The location associated with the key
     * @throws IOException if indexing fails
     */
    void indexRecord(String indexKey, String location) throws IOException;

    /**
     * Indexes a record and commits immediately.
     *
     * @param indexKey The key to index
     * @param location The location associated with the key
     * @throws IOException if indexing fails
     */
    void indexAndCommit(String indexKey, String location) throws IOException;

    /**
     * Finds matching index entries for a key or pattern.
     *
     * @param indexPattern The key or wildcard pattern
     * @return List of matching index entries
     * @throws Exception if query fails
     */
    List<LuceneIndexHelper.MatchResult> findLocationsForIndex(String indexPattern) throws Exception;

    /**
     * Deletes index entries matching a pattern.
     *
     * @param indexPattern The key or wildcard pattern
     * @throws IOException if deletion fails
     */
    void deleteKeys(String indexPattern) throws IOException;

    /**
     * Archives the index directory (e.g., tar.gz or zip).
     *
     * @throws IOException if archiving fails
     */
    void archiveIndex() throws IOException;

    /**
     * Closes the index, releasing resources.
     *
     * @throws IOException if closing fails
     */
    void close() throws IOException;
}
