package com.ross.serializer;

import org.springframework.boot.test.context.SpringBootTest;

import com.ross.serializer.stategy.ArchiveJobParams;
import com.ross.serializer.stategy.AvroFileSystemStrategy;
import com.ross.serializer.stategy.LuceneIndexHelper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestMethodOrder;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;


@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
class AvroApplicationArchiverTests {

	@Test
	void contextLoads() {}

    @Test
    @org.junit.jupiter.api.Order(1)
    void testWrite() {
        try {
            System.out.println("[begin:testWrite]");

            AvroFileSystemStrategy<OrderAvro> strategy =
                new AvroFileSystemStrategy<>("OrderJob");

            AtomicBoolean alreadyRun = new AtomicBoolean(false);

            strategy.write(
                OrderAvro.getClassSchema(),
                () -> alreadyRun.getAndSet(true)
                    ? Collections.emptyList()
                    : Order.getAvroOrders(15)
            );
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println();
        }
    }

    @Test
    void testRead() {
        try {
            System.out.println("[begin:testRead]");            
            final int[] count = {0};

            new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .read(OrderAvro.getClassSchema(), order -> {
                    OrderAvro o = (OrderAvro) order;
                    System.out.print("$"); // e.g. o.getOrderId();
                    return ++count[0] < 10;  // stop after n reads
                });

        } catch (Exception e) {
            e.printStackTrace();
        } finally 
        { System.out.println(); }
    } 

    @Test
    void testFindOrderAvroCurrentJob2() {
        try {
            System.out.println("[testFindOrderAvroCurrentJob2]");
            List<OrderAvro> matches = new ArrayList<>();

            new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .findMatchingRecords(
                    OrderAvro.class,
                    order -> order.getOrderId().toString().startsWith("ORDER-3"), // match predicate
                    order -> {
                        System.out.print("$");
                        matches.add(order);
                        return false; // stop after first match
                    }
                );

            System.out.println("Total matches collected: " + matches.size());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }    

 
    @Test
    void testBatchedRead() {
        try {
            System.out.println("[begin:testBatchedRead]");

            final int[] batchCount = {0};

            new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .readBatched(OrderAvro.getClassSchema(), (List<OrderAvro> batch) -> {
                    batch.forEach(order -> System.out.print("$")); 
                    return ++batchCount[0] < 5; // Stop after n batches
                });
            System.out.println();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    void testWriteAndIndex() {
        // mutable holders so lambda can reference them
        AtomicReference<LuceneIndexHelper> indexHelperRef = new AtomicReference<>();
        AtomicBoolean indexingAvailable = new AtomicBoolean(true);

        System.out.println("[begin:testWriteAndIndex]");

        try {
            // Initialize index helper (sidecar)
            LuceneIndexHelper ih = new LuceneIndexHelper(Paths.get("order-indexer"));
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
                    LuceneIndexHelper ih = indexHelperRef.get();
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
            LuceneIndexHelper ih = indexHelperRef.get();
            if (indexingAvailable.get() && ih != null) {
                try {
                    ih.close();
                } catch (Exception ex) {
                    System.err.println("Failed to close index helper: " + ex.getMessage());
                }
            }
            System.out.println();
        }
    }

    
    /*void testWriteAndIndex() {
        System.out.println("[begin:testWriteAndIndex]");

        AvroFileSystemStrategy<OrderAvro> strategy = new AvroFileSystemStrategy<>("OrderJob");
        String location = strategy.getJobParams().getNaming();
        AtomicBoolean alreadyRun = new AtomicBoolean(false);
        AtomicReference<List<OrderAvro>> writtenOrders = new AtomicReference<>();

        Supplier<List<OrderAvro>> indexAwareSupplier = () -> {
            if (alreadyRun.getAndSet(true)) return Collections.emptyList();
            List<OrderAvro> orders = Order.getAvroOrders(5);
            writtenOrders.set(orders);
            return orders;
        };

        try {
            strategy.write(OrderAvro.getClassSchema(), indexAwareSupplier);
        } catch (Exception e) {
            System.err.println("Write failed: " + e.getMessage());
            e.printStackTrace();
        }

        try (GenericIndexHelper indexHelper = new GenericIndexHelper(Paths.get("order-indexer"))) {
            indexHelper.open();

            List<OrderAvro> orders = writtenOrders.get();
            if (orders != null) {
                for (OrderAvro order : orders) {
                    indexHelper.indexRecord(order.getOrderId().toString(), location);
                }
            }

        } catch (Exception e) {
            System.err.println("Indexing failed or skipped: " + e.getMessage());
        }

        System.out.println();
    }*/

    @Test
    void testCompletedArchives() {
        System.out.println("[begin:testCompletedArchives]");
        // Given: a mix of past and current archive files
        List<String> archives = Arrays.asList(
            "order-archive-2025W24.avro", // Past week
            "order-archive-2025W33.avro", // Current week
            "order-archive-2024W52.avro"  // Last year
        );

        ArchiveJobParams.ArchiveNameResolver resolver =  
            ArchiveJobParams.getInstance("OrderJob").getArchiveNameResolver();

        // When: we resolve completed archives for weekly schedule
        List<String> completed = resolver.findCompletedArchives(
            archives,
            "order-archive",
            ArchiveJobParams.ArchiveSchedule.WEEKLY
        );


        AvroFileSystemStrategy<OrderAvro> strategy = new AvroFileSystemStrategy<>("OrderJob");
        strategy.put("test");

        // Then: current week's archive should NOT be included
//        assertFalse(completed.contains("order-archive-2025W25.avro"), 
//            "Current week archive should not be marked as completed");

        // And: past weeks should be included
//        assertTrue(completed.contains("order-archive-2025W24.avro"),
//            "Past week should be marked as completed");
//        assertTrue(completed.contains("order-archive-2024W52.avro"),
//            "Previous year archive should be marked as completed");

        // Optional: verify total count
 //       assertEquals(2, completed.size(), "Only past archives should be returned");

        System.out.println("Completed archives: " + completed);
    }

    @Test
    void testLucerneIndex() {
        System.out.println("[begin:testLucerneIndex]");

        try (LuceneIndexHelper helper = new LuceneIndexHelper(Paths.get("order-indexer"))) {
            //helper.open(); // or move this logic into the constructor       
            helper.indexAndCommit("myTestKey", "myTestFile.avro");
            helper.findLocationsForIndex("myTestKey").forEach(result ->
                System.out.println("Index: " + result.getIndex() + 
                    ", File: " + result.getLocation())
            );
            helper.deleteKeys("myTestKey*");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testReadAllOrders()  {
        try {
            System.out.println("[begin:testReadAllOrders]");
            new AvroFileSystemStrategy<OrderAvro>("order-archive-2025W28.avro")
                .readAll(
                    OrderAvro.getClassSchema(),
                    (List<OrderAvro> orders) -> {
                        //orders.forEach(order -> System.out.print(order.getOrderId()+", "));
                        orders.forEach(order -> System.out.print("$"));
                    }
                );
        } catch (Exception e) {
            e.printStackTrace();
        } finally 
        { System.out.println(); }
    }      

    @Test
    void testFindOrderAvroCurrentJob1() {
        final int[] count = {0};
        try {
            System.out.println("[testFindOrderAvroCurrentJob1]");
            Optional<OrderAvro> match = 
                new AvroFileSystemStrategy<OrderAvro>("OrderJob")            
            .find(
                OrderAvro.getClassSchema(),
                order -> {
                    if (order.getShipping() > 4000) {
                        System.out.print("$");
                        return ++count[0] > 3;
                    }
                    return false;
                }
            );
            match.ifPresent(order -> {
                System.out.println("Matched shipping > 4000: id " + order.getOrderId() + " shipping " + order.getShipping());
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

   /*@Test
    @org.junit.jupiter.api.Order(1)
    void testWrite() {
        try {
            System.out.println("[begin:testWrite]");            
            AvroFileSystemStrategy<OrderAvro> strategy =
                new AvroFileSystemStrategy<>("OrderJob");

            final int[] count = {0};

            strategy.write(
                OrderAvro.getClassSchema(),
                () -> ++count[0] < 4
                    ? Order.getAvroOrders(15) 
                    : Collections.emptyList()
            );
        } catch (Exception e) {
            e.printStackTrace();
        } finally { System.out.println(); }
    }*/

    /* @Test
    @org.junit.jupiter.api.Order(2)
    void testWriteAndIndex() {
        System.out.println("[begin:testWriteAndIndex]");

        GenericIndexHelper indexHelper = null;
        boolean indexingAvailable = true;

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
    }*/

/* 
    @Test
    @org.junit.jupiter.api.Order(3)
    void testFindOrderFromIndexEntry() {
        System.out.println("[begin:testFindOrderFromIndexEntry]");

        final String orderIdToSearch = "ORDER-51*"; // Adjust as needed

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
*/



    
    /*@Test
    @org.junit.jupiter.api.Order(2)
    void testWriteAndIndexLambda() {
        try {
            System.out.println("[begin:testWriteAndIndex]");
            
            AvroFileSystemStrategy<OrderAvro> strategy = new AvroFileSystemStrategy<>("OrderJob");
            String location = strategy.getJobParams().getNaming(); 

            GenericIndexHelper indexHelper = new GenericIndexHelper(Paths.get("order-indexer"));
            indexHelper.open();

            final int[] count = {0};

            strategy.write(
                OrderAvro.getClassSchema(),
                () -> {
                    if (++count[0] < 5) {
                        List<OrderAvro> orders = Order.getAvroOrders(5);

                        orders.forEach(order -> {
                            try {
                                indexHelper.indexRecord(order.getOrderId().toString(), location);
                            } catch (Exception e) {
                                throw new RuntimeException("Indexing failed for orderId: " + order.getOrderId(), e);
                            }
                        });
                        return orders;
                    } else {
                        return Collections.emptyList();
                    }
                }
            );
            indexHelper.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println();
        }
    }*/
   

     /*@Test
    void testLucerneHelper() {
        try {
            System.out.println("[begin:testLucerneHelper]");
            List<GenericIndexHelper.MatchResult> results = new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .indexerFind("ORDER-27*")
                .orElse(Collections.emptyList());

            for (GenericIndexHelper.MatchResult result : results) {
                System.out.println("Index: " + result.getIndex() + ", File: " + result.getLocation());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }   */   
}
