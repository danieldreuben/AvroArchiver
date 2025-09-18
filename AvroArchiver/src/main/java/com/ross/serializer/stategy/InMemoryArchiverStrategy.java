package com.ross.serializer.stategy;


import org.apache.avro.Schema;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.specific.SpecificRecord;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.*;

public class InMemoryArchiverStrategy implements ArchiverStrategy {

    // in-memory "store" by schema full name
    private final Map<String, List<SpecificRecord>> store = new HashMap<>();

    @Override
    public <T extends SpecificRecord> void read(Schema schema, Function<T, Boolean> recordHandler) throws IOException {
        List<SpecificRecord> records = store.getOrDefault(schema.getFullName(), List.of());
        for (SpecificRecord record : records) {
            @SuppressWarnings("unchecked")
            T cast = (T) record;
            boolean cont = recordHandler.apply(cast);
            if (!cont) break;
        }
    }

    @Override
    public <T extends SpecificRecord> void write(Schema schema, Supplier<List<T>> recordSupplier) throws IOException {
        store.computeIfAbsent(schema.getFullName(), k -> new CopyOnWriteArrayList<>())
                .addAll(recordSupplier.get());
    }

    @Override
    public <T extends SpecificRecord> void write(Schema schema, Supplier<List<T>> recordSupplier, File file) {
        // in-memory only → ignore File target
        store.computeIfAbsent(schema.getFullName(), k -> new CopyOnWriteArrayList<>())
                .addAll(recordSupplier.get());
    }

    @Override
    public <T extends SpecificRecord> Optional<T> find(Schema schema, Predicate<T> recordFilter) throws IOException {
        List<SpecificRecord> records = store.getOrDefault(schema.getFullName(), List.of());
        for (SpecificRecord record : records) {
            @SuppressWarnings("unchecked")
            T cast = (T) record;
            if (recordFilter.test(cast)) {
                return Optional.of(cast);
            }
        }
        return Optional.empty();
    }

    @Override
    public <T extends SpecificRecord> void readAll(Schema schema, Consumer<List<T>> recordHandler) throws IOException {
        @SuppressWarnings("unchecked")
        List<T> records = (List<T>) store.getOrDefault(schema.getFullName(), List.of());
        recordHandler.accept(records);
    }

    @Override
    public <T extends SpecificRecord> void readBatched(Schema schema, Function<List<T>, Boolean> recordHandler) throws IOException {
        @SuppressWarnings("unchecked")
        List<T> records = (List<T>) store.getOrDefault(schema.getFullName(), List.of());
        // For demo, just send entire list as one batch
        recordHandler.apply(records);
    }

    @Override
    public <T extends SpecificRecord> void findMatchingRecords(Class<T> clazz, Predicate<T> recordFilter, Function<T, Boolean> onMatch) {
        store.values().forEach(records -> {
            for (SpecificRecord record : records) {
                if (clazz.isInstance(record)) {
                    T cast = clazz.cast(record);
                    if (recordFilter.test(cast)) {
                        Boolean cont = onMatch.apply(cast);
                        if (!cont) return;
                    }
                }
            }
        });
    }

    @Override
    public <T extends SpecificRecord> long fastCountRecords(Class<T> clazz, SeekableInput input) {
        // Not meaningful in memory, just count store
        return store.values().stream()
                .flatMap(List::stream)
                .filter(clazz::isInstance)
                .count();
    }

    // utility: clear state
    public void clear() {
        store.clear();
    }
}
