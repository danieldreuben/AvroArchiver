package com.ross.serializer;

import java.io.IOException;
import java.nio.file.Paths;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

import com.ross.serializer.stategy.AvroFileSystemStrategy;
import com.ross.serializer.stategy.MatchResult;
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
            indexer.setKeyExtractor(OrderAvro::getOrderId); // lamda syntax: record -> record.getOrderId()
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
        }
    }    
}

