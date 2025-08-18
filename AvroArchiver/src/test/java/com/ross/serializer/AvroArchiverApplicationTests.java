package com.ross.serializer;

import org.springframework.boot.test.context.SpringBootTest;

import com.ross.serializer.stategy.ArchiveJobParams;
import com.ross.serializer.stategy.AvroFileSystemStrategy;
import com.ross.serializer.stategy.IndexerPlugin;
import com.ross.serializer.stategy.LuceneIndexHelper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
class AvroApplicationArchiverTests {

	@Test
	void contextLoads() {}

    @Test
    @org.junit.jupiter.api.Order(1)
    void testWrite() {
        LuceneIndexHelper<OrderAvro> indexer = null;

        try {
            System.out.println("[begin:testWrite]");

            AvroFileSystemStrategy<OrderAvro> strategy =
                new AvroFileSystemStrategy<>("OrderJob");

            indexer = new LuceneIndexHelper<>(Paths.get("order-indexer"));
            indexer.setKeyExtractor(OrderAvro::getOrderId); // lamda syntax: record -> record.getOrderId()
            strategy.setIndexer(indexer);

            Assertions.assertTrue(Files.exists(Paths.get("order-indexer")), "Index path should exist");
            //Assertions.assertTrue(Files.list(Paths.get("order-indexer")).findAny().isPresent(), "Index directory should not be empty");            

            AtomicBoolean alreadyRun = new AtomicBoolean(false);

            strategy.write(
                OrderAvro.getClassSchema(),
                () -> alreadyRun.getAndSet(true)
                    ? Collections.emptyList()
                    : Order.getAvroOrders(50)
            );
        } catch (Exception e) {
            Assertions.fail("Exception during testWrite: " + e.getMessage(), e);
        } finally {
            if (indexer != null) {
                try { indexer.close(); } catch (IOException ignore) {}
            }            
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
            Assertions.fail("Exception during testRead: " + e.getMessage(), e);
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
                        // stop after 5 matches
                        return matches.size() >= 5;
                    }
                );

            System.out.println("\nTotal matches collected: " + matches.size());

        } catch (Exception e) {
            Assertions.fail("Exception during testFindOrderAvroCurrentJob2: " + e.getMessage(), e);
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
            Assertions.fail("Exception during testBatchedRead: " + e.getMessage(), e);
        }
    }
    
    /**
     * Unit test for archiving a Lucene index directory to a compressed TAR (tar.gz) file.
     * <p>
     * This test:
     * <ul>
     *   <li>Loads {@link ArchiveJobParams} for the {@code OrderJob} configuration.</li>
     *   <li>Resolves the source Lucene index directory path.</li>
     *   <li>Resolves the destination archive directory path.</li>
     *   <li>Invokes {@link LuceneIndexHelper#archiveDirectoryToTarGz(String, String)} 
     *       to create a {@code .tar.gz} archive of the index.</li>
     * </ul>
     * <p>
     * Expected behavior:
     * <ul>
     *   <li>The archive file should be created in the specified destination directory.</li>
     * </ul>
     * <p>
     * You can verify the archive manually on the command line with:
     * <pre>{@code
     *   tar -tvzf ./archives/order-indexer.tar.gz
     * }</pre>
     *
     * If any exception occurs, the test fails with a descriptive message.
     */        
    @Test
    void testTarIndex() {
        System.out.println("[begin:testTarIndex]");
        try {
            ArchiveJobParams jobParams = ArchiveJobParams.getInstance("OrderJob");   

            String srcIndex = jobParams.getIndexer().getName();
            String destTar = jobParams.getStorage().getFile().getArchiveDir();

            System.out.println("archive " + jobParams.getStorage().getFile().getArchiveDir());
            System.out.println("indexer " + jobParams.getIndexer().getName());                       

            LuceneIndexHelper.archiveDirectoryToTarGz(srcIndex, destTar);

        } catch (Exception e) {
            Assertions.fail("Failed to TAR index : " + e.getMessage(), e);
        }    
    }

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

        try (LuceneIndexHelper<OrderAvro> helper =
                new LuceneIndexHelper<>(Paths.get("order-indexer"))) {

            // Insert
            helper.indexAndCommit("myTestKey", "myTestFile.avro");

            // Search
            List<LuceneIndexHelper.MatchResult> results =
                helper.findLocationsForIndex("myTestKey");

            // ✅ Assertions
            Assertions.assertFalse(results.isEmpty(),
                "Expected to find at least one result for key 'myTestKey'");
            Assertions.assertTrue(
                results.stream().anyMatch(r -> "myTestFile.avro".equals(r.getLocation())),
                "Expected location 'myTestFile.avro' to be in search results"
            );

            // Cleanup
            helper.deleteKeys("myTestKey*");

            // ✅ Assert cleanup worked
            List<LuceneIndexHelper.MatchResult> afterDelete =
                helper.findLocationsForIndex("myTestKey");

            Assertions.assertTrue(afterDelete.isEmpty(),
                "Expected no results after deleting key 'myTestKey*'");

        } catch (Exception e) {
            Assertions.fail("Exception during testLucerneIndex: " + e.getMessage(), e);
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
            Assertions.fail("Exception during testReadAllOrders: " + e.getMessage(), e);
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
                            if (order.getShipping() > 3000) {
                                System.out.print("$");
                                return ++count[0] > 5; // stop after more than n matches
                            }
                            return false;
                        }
                    );

            match.ifPresent(order -> {
                System.out.println("\nMatched shipping > 3000 - " + count[0] 
                    + " match(es): id " + order.getOrderId());
            });

            if (match.isEmpty()) {
                System.out.println("\nNo match found after checking " + count[0] + " potential matches.");
            }

        } catch (Exception e) {
            Assertions.fail("Exception during testReadAllOrders: " + e.getMessage(), e);
        }
    }
}
