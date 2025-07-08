package com.ross.excel.serializer;

import org.springframework.boot.test.context.SpringBootTest;

import com.ross.excel.serializer.archiver.AvroFileSystemStrategy;
import com.ross.excel.serializer.archiver.GenericIndexHelper;

import org.springframework.beans.factory.annotation.Autowired;
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

	@Autowired
    //private AvroAchiver archiverApp;

	@Test
	void contextLoads() {
	}

    /*@Test
    @org.junit.jupiter.api.Order(1)
    void writeOrdersToArchive() {
        System.out.println("[writeOrdersToArchive]");
        new OrderJob().testWriteOrders();
    }

    @Test
    void readOrdersFromArchive() {
        System.out.println("[readOrdersFromArchive]");
        new OrderJob().testReadOrders();
    }*/

    @Test
    void findAvroFile() {
        try {
            System.out.println("[begin:findAvroFile]"); 
            List<String> items = new AvroFileSystemStrategy<OrderAvro>("OrderJob2")
                .find("ORDER-9183")
                .orElse(Collections.emptyList());
            items.forEach(System.out::println);
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
                    System.out.print("$");
                    // e.g. o.getOrderId();
                    return ++count[0] < 10; // stop after n results
                });

        } catch (Exception e) {
            e.printStackTrace();
        } finally 
        { System.out.println(); }
    }
  
    /*@Test
    void testRead2() {
        System.out.println("[begin:testRead2]");

        try {
            final int[] count = {0};

            new AvroFileSystemStrategy<OrderAvro>("order-archive-2025W27.avro")
                .read(OrderAvro.getClassSchema(), order -> {
                    OrderAvro o = (OrderAvro) order;
                    System.out.println("Read order: " + o.getOrderId());
                    return ++count[0] < 10; // stop after reading 10 records
                });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }  */  

    @Test
    void testReadAllOrders()  {
        System.out.println("[begin:testReadAllOrders]");
        try {
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
    void testWrite() {
        System.out.println("[begin:testWrite]");

        try {
            AvroFileSystemStrategy<OrderAvro> strategy =
                new AvroFileSystemStrategy<>("OrderJob2");

            final int[] count = {0};

            strategy.write(
                OrderAvro.getClassSchema(),
                () -> ++count[0] < 4
                    ? Order.getSerializableOrders(5)  
                    : Collections.emptyList()
            );

        } catch (Exception e) {
            e.printStackTrace();
        } finally 
        { System.out.println(); }
    }

    @Test
    void testReadBatched()  {
        try {
            System.out.println("[begin:testReadBatched]");
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

    /*@Test
    void testReadBatched2() {
        System.out.println("[begin:testReadBatched2]");

        try {
            AvroFileSystemStrategy<OrderAvro> strategy =
                new AvroFileSystemStrategy<>("order-archive-2025W27.avro");

            final int[] total = {0};

            strategy.readBatched(
                OrderAvro.getClassSchema(),
                batch -> {
                    System.out.println("Received batch of size: " + batch.size());

                    batch.forEach(order -> System.out.println(" - " + ((OrderAvro) order).getOrderId().toString()));
                    //total[0] += batch.size();
                    return ++total[0] < 5; // Stop after 20 records
                }
            );

            System.out.println("[end:testReadBatched2]");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/



    @Test
    void testFindOrder() {
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
    void testFindOrders() {
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
