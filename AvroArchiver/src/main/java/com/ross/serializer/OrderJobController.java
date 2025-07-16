package com.ross.serializer;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.springframework.stereotype.Component;

import com.ross.serializer.stategy.AvroFileSystemStrategy;
import com.ross.serializer.stategy.GenericIndexHelper;
import com.ross.serializer.stategy.GenericIndexHelper.MatchResult;

@Component
public class OrderJobController {

    public OrderJobController() {} 

    public void run() {
        System.out.println("Running scheduled task...");
        testWriteAndIndex();
    }

    void testWriteOrders()  {
        try {
            final int[] count = {0};        
            new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .write (
                    OrderAvro.getClassSchema(),
                    () -> {
                        return ++count[0] < 5 ? // simulates a batch write
                            Order.getAvroOrders(5) : new ArrayList<>();
                    }
                );
        } catch (Exception e) {}
    } 

    void testReadOrders() {
        try {
            final int[] count = {0};

            new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .read(OrderAvro.getClassSchema(), order -> {
                    Order s = new Order();
                    s.setAvroOrder((OrderAvro) order);
                    System.out.println(s.toString());
                    return ++count[0] < 25; // stop after N..
                });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void testFindOrderFromIndexEntry() {

        final String orderIdToSearch = "ORDER-51*"; // Adjust as needed
        System.out.println("[begin:testFindOrderFromIndexEntry]");

        try (GenericIndexHelper indexHelper = new GenericIndexHelper(Paths.get("order-indexer"))) {
            indexHelper.open();

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
        GenericIndexHelper indexHelper = null;
        boolean indexingAvailable = true;
        System.out.println("[begin:testWriteAndIndex]");

        try {
            // Initialize index helper (sidecar)
            indexHelper = new GenericIndexHelper(Paths.get("order-indexer"));
            indexHelper.open();
        } catch (Exception e) {
            System.err.println("Index helper failed to initialize: " + e.getMessage());
            indexingAvailable = false;
        }

        try {
            AvroFileSystemStrategy<OrderAvro> strategy = new AvroFileSystemStrategy<>("OrderJob");
            String location = strategy.getJobParams().getNaming();
            AtomicBoolean alreadyRun = new AtomicBoolean(false);
            AtomicReference<List<OrderAvro>> writtenOrders = new AtomicReference<>();

            // Supplier to provide records (not index yet)
            Supplier<List<OrderAvro>> indexAwareSupplier = () -> {
                if (alreadyRun.getAndSet(true)) {
                    return Collections.emptyList(); // terminate
                }
                List<OrderAvro> orders = Order.getAvroOrders(5);
                writtenOrders.set(orders); // capture for later indexing
                return orders;
            };

            // Try to write records
            strategy.write(OrderAvro.getClassSchema(), indexAwareSupplier);

            // Index only after successful write
            if (indexingAvailable) {
                List<OrderAvro> ordersToIndex = writtenOrders.get();
                if (ordersToIndex != null) {
                    for (OrderAvro order : ordersToIndex) {
                        try {
                            indexHelper.indexRecord(order.getOrderId().toString(), location);
                        } catch (Exception ex) {
                            System.err.println("Failed to index orderId " + order.getOrderId() + ": " + ex.getMessage());
                            // Optionally continue or throw depending on severity
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (indexingAvailable && indexHelper != null) {
                try {
                    indexHelper.close();
                } catch (Exception ex) {
                    System.err.println("Failed to close index helper: " + ex.getMessage());
                }
            }
            System.out.println();
        }
    }
    /*public void setRecordsFromArchive(List<SpecificRecord> t) {

        List<OrderAvro> orderList = t.stream()
            .filter(OrderAvro.class::isInstance)
            .map(OrderAvro.class::cast)
            .collect(Collectors.toList());

        orderList.stream().forEach(System.out::println);
    }*/

}

