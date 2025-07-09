package com.ross.excel.serializer;

import org.springframework.boot.test.context.SpringBootTest;


import com.ross.excel.serializer.archiver.AvroFileSystemStrategy;
import com.ross.excel.serializer.archiver.GenericIndexHelper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestMethodOrder;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;


@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
class AvroApplicationArchiverTests {

	@Test
	void contextLoads() {
	}


    @Test
    @org.junit.jupiter.api.Order(1)
    void testWrite() {
        try {
            System.out.println("[begin:testWrite]");            
            AvroFileSystemStrategy<OrderAvro> strategy =
                new AvroFileSystemStrategy<>("OrderJob2");

            final int[] count = {0};

            strategy.write(
                OrderAvro.getClassSchema(),
                () -> ++count[0] < 4
                    ? Order.getAvroOrders(5) 
                    : Collections.emptyList()
            );
        } catch (Exception e) {
            e.printStackTrace();
        } finally { System.out.println(); }
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    void testWriteAndIndex() {
        System.out.println("[begin:testWriteAndIndex]");

        try (GenericIndexHelper indexHelper = new GenericIndexHelper(Paths.get("order-indexer"))) {
            indexHelper.open();

            AvroFileSystemStrategy<OrderAvro> strategy = new AvroFileSystemStrategy<>("OrderJob2");
            String location = strategy.getJobParams().getNaming();

            AtomicBoolean alreadyRun = new AtomicBoolean(false);

            Supplier<List<OrderAvro>> indexAwareSupplier = () -> {
                if (alreadyRun.getAndSet(true)) {
                    return Collections.emptyList(); // terminate
                }

                List<OrderAvro> orders = Order.getAvroOrders(5);
                for (OrderAvro order : orders) {
                    try {
                        indexHelper.indexRecord(order.getOrderId().toString(), location);
                    } catch (Exception e) {
                        System.err.println("Failed to index orderId " + order.getOrderId());
                        throw new RuntimeException("Indexing failed", e);
                    }
                }
                return orders;
            };

            strategy.write(OrderAvro.getClassSchema(), indexAwareSupplier);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println();
        }
    }

    @Test
    void testFindOrderAvroCurrentJob2() {
        try {
            System.out.println("[testFindOrderAvroCurrentJob2]");
            List<OrderAvro> matches = new ArrayList<>();

            new AvroFileSystemStrategy<OrderAvro>("OrderJob2")
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
    void testLucerneIndex() {
        System.out.println("[begin:testLucerneIndex]");

        try (GenericIndexHelper helper = new GenericIndexHelper(Paths.get("order-indexer"))) {
            helper.open(); // or move this logic into the constructor
            
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
    void testRead() {
        try {
            System.out.println("[begin:testRead]");            
            final int[] count = {0};

            new AvroFileSystemStrategy<OrderAvro>("OrderJob2")
                .read(OrderAvro.getClassSchema(), order -> {
                    OrderAvro o = (OrderAvro) order;
                    System.out.print("$"); // e.g. o.getOrderId();
                    return ++count[0] < 10;  // stop after n results
                });

        } catch (Exception e) {
            e.printStackTrace();
        } finally 
        { System.out.println(); }
    } 

    @Test
    void testReadAllOrders()  {
        try {
            System.out.println("[begin:testReadAllOrders]");
            new AvroFileSystemStrategy<OrderAvro>("order-archive-2025W27.avro")
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
    void testBatchedRead() {
        try {
            System.out.println("[begin:testBatchedRead]");

            final int[] batchCount = {0};

            new AvroFileSystemStrategy<OrderAvro>("OrderJob2")
                .readBatched(OrderAvro.getClassSchema(), (List<OrderAvro> batch) -> {
                    batch.forEach(order -> System.out.print("$")); 
                    return ++batchCount[0] < 5; // Stop after n batches
                });

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println();
        }
    }

    @Test
    void testFindOrderAvroCurrentJob1() {
        final int[] count = {0};
        try {
            System.out.println("[testFindOrderAvroCurrentJob1]");
            Optional<OrderAvro> match = 
                new AvroFileSystemStrategy<OrderAvro>("OrderJob2")            
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
    @org.junit.jupiter.api.Order(2)
    void testWriteAndIndexLambda() {
        try {
            System.out.println("[begin:testWriteAndIndex]");
            
            AvroFileSystemStrategy<OrderAvro> strategy = new AvroFileSystemStrategy<>("OrderJob2");
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
            List<GenericIndexHelper.MatchResult> results = new AvroFileSystemStrategy<OrderAvro>("OrderJob2")
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
