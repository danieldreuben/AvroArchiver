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


@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
class AvroApplicationArchiverTests {

	@Test
	void contextLoads() {
	}

    @Test
    void testLucerneRecords() {
        try {
            System.out.println("[begin:testLucerneRecords]");
            GenericIndexHelper helper = new GenericIndexHelper(Paths.get("order-indexer"));
            helper.open();
            helper.indexRecord("myTestKey","myTestFile.avro");
            helper.findLocationsForIndex("myTestKey").forEach(result ->
                System.out.println("Index: " + result.getIndex() + 
                    ", File: " + result.getLocation())
            );
            helper.deleteKeys("myTestKey*");
            helper.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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

    @Test
    void testRead() {
        try {
            System.out.println("[begin:testRead]");            
            final int[] count = {0};

            new AvroFileSystemStrategy<OrderAvro>("OrderJob2")
                .read(OrderAvro.getClassSchema(), order -> {
                    OrderAvro o = (OrderAvro) order;
                    System.out.print("$");
                    // e.g. o.getOrderId();
                    return ++count[0] < 10; // stop after n results
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
    void testWriteAndIndexLambda() {
        System.out.println("[begin:testWriteAndIndex]");

        try {
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
    }


    @Test
    void testBatchedRead()  {
        try {
            System.out.println("[begin:testBatchedRead]");
            final int[] count = {0};

            new AvroFileSystemStrategy<OrderAvro>("OrderJob2")
                .readBatched(OrderAvro.getClassSchema(), (List<OrderAvro> batch) -> {

                    batch.forEach(order -> System.out.print("$"));
                    if (batch.size() > 2) { 
                        return false; 
                    }                    
                    return ++count[0] < 2; // stop after 2 batches
                });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println();
        }
    }       

    @Test
    void testFindOrderCurrentJob1() {
        final int[] count = {0};
        try {
            System.out.println("[testFindOrder]");
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

    @Test
    void testFindOrderCurrentJob2() {
        final int[] count = {0}; // mutable counter
        try {
            System.out.println("[testFindOrders]");
            List<OrderAvro> matches = new ArrayList<>();

            new AvroFileSystemStrategy<OrderAvro>("OrderJob2")
                .findMatchingRecords(
                    OrderAvro.class,
                    order -> order.getOrderId().toString().startsWith("ORDER-3"),
                    order -> {
                        System.out.print("$");
                        matches.add(order);
                        return count[0]++ < 5; // stop after 5 matches
                    }
                );

            System.out.println("Total matches collected: " + matches.size());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }     
}
