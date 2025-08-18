package com.ross.serializer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.springframework.stereotype.Component;

import com.ross.serializer.stategy.AvroFileSystemStrategy;
import com.ross.serializer.stategy.LuceneIndexHelper.MatchResult;
import com.ross.serializer.stategy.LuceneIndexHelper;

@Component
public class OrderJobController {

    public OrderJobController() {} 

    public void run() {
        System.out.println("Running scheduled task...");
        testWriteAndIndex();
    }

    void testFindOrderFromIndexEntry() {

        final String orderIdToSearch = "ORDER-51*"; // Adjust as needed
        System.out.println("[begin:testFindOrderFromIndexEntry]");

        try (LuceneIndexHelper<OrderAvro> indexHelper = new LuceneIndexHelper<>(Paths.get("order-indexer"))) {
            //indexHelper.open();

            // Step 1: Find index matches by orderId pattern
            List<MatchResult> matches = indexHelper.findLocationsForIndex(orderIdToSearch);

            if (matches.isEmpty()) {
                System.out.println("No index entries found for: " + orderIdToSearch);
                return;
            }

            for (MatchResult match : matches) {
                String location = match.getLocation();  // Assuming getLocation() method
                String index = match.getIndex();        // If useful

                System.out.println("Found index: " + index + ", file: " + location);

                AvroFileSystemStrategy<OrderAvro> strategy = new AvroFileSystemStrategy<>(location);

                // Step 2: Search the Avro file for matching orderId
                strategy.findMatchingRecords(
                    OrderAvro.class,
                    order -> index.equals(order.getOrderId().toString()),
                    order -> {
                        System.out.println("Matched order: " + order);
                        return true;  // continue searching if desired
                    }
                );
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void testWriteAndIndex() {
        LuceneIndexHelper<OrderAvro> indexer = null;

        try {
            System.out.println("[begin:testWrite]");

            AvroFileSystemStrategy<OrderAvro> strategy =
                new AvroFileSystemStrategy<>("OrderJob");

            indexer = new LuceneIndexHelper<>(Paths.get("order-indexer"));
            //indexer.setKeyExtractor(record -> record.getOrderId().toString());
            indexer.setKeyExtractor(OrderAvro::getOrderId);
            strategy.setIndexer(indexer);


            AtomicBoolean alreadyRun = new AtomicBoolean(false);

            strategy.write(
                OrderAvro.getClassSchema(),
                () -> alreadyRun.getAndSet(true)
                    ? Collections.emptyList()
                    : Order.getAvroOrders(50)
            );
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (indexer != null) {
                try { indexer.close(); } catch (IOException ignore) {}
            }            
            System.out.println();
        }
    }    

/* 
    void testWriteAndIndex() {
        // mutable holders so lambda can reference them
        AtomicReference<LuceneIndexHelper<OrderAvro>> indexHelperRef = new AtomicReference<>();
        AtomicBoolean indexingAvailable = new AtomicBoolean(true);

        System.out.println("[begin:testWriteAndIndex]");

        try {
            // Initialize index helper (sidecar)
            LuceneIndexHelper<OrderAvro> ih = new LuceneIndexHelper<>(Paths.get("order-indexer"));
            //ih.open();
            indexHelperRef.set(ih);
        } catch (Exception e) {
            System.err.println("Index helper failed to initialize: " + e.getMessage());
            indexingAvailable.set(false);
        }

        try {
            AvroFileSystemStrategy<OrderAvro> strategy = new AvroFileSystemStrategy<>("OrderJob");
            String location = strategy.getJobParams().getNaming();
            AtomicBoolean alreadyRun = new AtomicBoolean(false);

            // Supplier that both produces and indexes (indexes only if indexingAvailable)
            Supplier<List<OrderAvro>> indexAwareSupplier = () -> {
                if (alreadyRun.getAndSet(true)) {
                    return Collections.emptyList(); // terminate
                }

                List<OrderAvro> orders = Order.getAvroOrders(5);

                if (indexingAvailable.get()) {
                    LuceneIndexHelper<OrderAvro> ih = indexHelperRef.get();
                    if (ih != null) {
                        for (OrderAvro order : orders) {
                            try {
                                ih.indexRecord(order.getOrderId().toString(), location);
                            } catch (Exception ex) {
                                System.err.println("Failed to index orderId " + order.getOrderId() + ": " + ex.getMessage());
                            }
                        }
                    }
                }

                return orders;
            };

            // Write using the supplier (also triggers indexing)
            strategy.write(OrderAvro.getClassSchema(), indexAwareSupplier);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            LuceneIndexHelper<OrderAvro> ih = indexHelperRef.get();
            if (indexingAvailable.get() && ih != null) {
                try {
                    ih.close();
                } catch (Exception ex) {
                    System.err.println("Failed to close index helper: " + ex.getMessage());
                }
            }
            System.out.println();
        }
    } */


}

