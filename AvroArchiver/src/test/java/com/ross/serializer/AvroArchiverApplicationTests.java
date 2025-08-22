package com.ross.serializer;

import org.springframework.boot.test.context.SpringBootTest;

import com.ross.serializer.stategy.ArchiveJobParams;
import com.ross.serializer.stategy.AvroFileSystemStrategy;
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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
class AvroApplicationArchiverTests {
    private static final Logger log = LoggerFactory.getLogger(AvroApplicationArchiverTests.class);

	@Test
	void contextLoads() {}

    @Test
    @org.junit.jupiter.api.Order(1)
    void testWrite() {
        LuceneIndexHelper<OrderAvro> indexer = null;

        try {
            log.info("[begin:testWrite]");

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
            log.info("count " + strategy.count(OrderAvro.class));

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
            log.info("[begin:testRead]");            
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
            log.info("[testFindOrderAvroCurrentJob2]");
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

            log.info("\nTotal matches collected: " + matches.size());

        } catch (Exception e) {
            Assertions.fail("Exception during testFindOrderAvroCurrentJob2: " + e.getMessage(), e);
        }
    }
   
    @Test
    void testBatchedRead() {
        try {
            log.info("[begin:testBatchedRead]");

            final int[] batchCount = {0};

            new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .readBatched(OrderAvro.getClassSchema(), (List<OrderAvro> batch) -> {
                    batch.forEach(e -> System.out.print("$"));
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
        log.info("[begin:testTarIndex]");
        try {
            ArchiveJobParams jobParams = ArchiveJobParams.getInstance("OrderJob");   

            String srcIndex = jobParams.getIndexer().getName();
            String destTar = jobParams.getStorage().getFile().getArchiveDir();

            log.info("archive " + jobParams.getStorage().getFile().getArchiveDir());
            log.info("indexer " + jobParams.getIndexer().getName());                       

            LuceneIndexHelper.archiveDirectoryToTarGz(srcIndex, destTar);

        } catch (Exception e) {
            Assertions.fail("Failed to TAR index : " + e.getMessage(), e);
        }    
    }

    // Incomplete: Idea is to put archive(s) from current working folder but if put is used 
    // after write of current archive then no necessary.
    // 
    //@Test
    void testCompletedArchives() throws Exception {
        log.info("[begin:testCompletedArchives]");
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

        log.info("Completed archives: " + completed);
    }

    @Test
    void testLucerneIndex() {
        log.info("[begin:testLucerneIndex]");

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
            log.info("[begin:testReadAllOrders]");
            new AvroFileSystemStrategy<OrderAvro>("./order-archive-2025W28.avro")
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
            log.info("[testFindOrderAvroCurrentJob1]");
            Optional<OrderAvro> match = 
                new AvroFileSystemStrategy<OrderAvro>("OrderJob")            
                    .find(
                        OrderAvro.getClassSchema(),
                        order -> {
                            if (order.getShipping() > 4000) {
                                System.out.print("$");
                                return ++count[0] > 5; // stop after more than n matches
                            }
                            return false;
                        }
                    );

            match.ifPresent(order -> {
                log.info("\nMatched " + count[0] 
                    +" where shipping > 4000 " + order);
            });

            if (match.isEmpty()) {
                log.info("\nNo match found after checking " + count[0] + " potential matches.");
            }

        } catch (Exception e) {
            Assertions.fail("Exception during testReadAllOrders: " + e.getMessage(), e);
        }
    }
}
