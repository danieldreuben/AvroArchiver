package com.ross.excel.serializer;

import org.springframework.boot.test.context.SpringBootTest;

import com.ross.excel.serializer.archiver.AvroFileSystemStrategy;

import org.springframework.beans.factory.annotation.Autowired;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Order;

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

    @Test
    @Order(1)
    void writeOrdersToArchive() {
        System.out.println("[writeOrdersToArchive]");
        new OrderJob().testWriteOrders();
    }

    @Test
    void readOrdersFromArchive() {
        System.out.println("[readOrdersFromArchive]");
        new OrderJob().testReadOrders();
    }

    void testRead() {
        try {
            System.out.println("[begin:testRead]");            
            final int[] count = {0};

            new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .read(OrderAvro.getClassSchema(), order -> {
                    OrderAvro o = (OrderAvro) order;
                    // e.g. o.getOrderId();
                    return ++count[0] < 10; // stop after 3 results
                });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testReadAllOrders()  {
        System.out.println("[begin:testReadAllOrders]");
        try {
            new AvroFileSystemStrategy<OrderAvro>("order-archive-2025W27.avro")
                .readAll(
                    OrderAvro.getClassSchema(),
                    (List<OrderAvro> orders) -> {
                        //orders.forEach(order -> System.out.print(order.getOrderId()+", "));
                        orders.forEach(order -> System.out.print("."));
                    }
                );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testBatchedRead()  {
        try {
            System.out.println("[begin:testBatchedRead]");
            final int[] count = {0};

            new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .readBatches(OrderAvro.getClassSchema(), (List<OrderAvro> batch) -> {

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
    void testFindOrders() {
        final int[] count = {0};
        try {
            System.out.println("[testFindOrders]");
            Optional<OrderAvro> match = 
            new AvroFileSystemStrategy<OrderAvro>("OrderJob")            
            .findMatchingRecords(
                OrderAvro.getClassSchema(),
                order -> {
                    if (order.getShipping() > 4000) {
                        System.out.println("Matched shipping > 4000: id " + order.getOrderId() + " shipping " + order.getShipping());
                        return ++count[0] > 3;
                    }
                    return false;
                }
            );

            match.ifPresent(order -> {
                //System.out.println("Found: " + order);
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*@Test
    void testFindFirstOrder() {
        try {
            System.out.println("[testFindOrder]");
            Optional<OrderAvro> result = new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .findMatchingRecords(
                    OrderAvro.getClassSchema(),
                    order -> order.getOrderId().toString().startsWith("ORDER-8")
                );

            result.ifPresent(order -> System.out.println("Found order: " + order));
            
        } catch (Exception e) {
            e.printStackTrace();
        }
   }  */

    /*@Test
    void testFindOrders()  {
        try {
            new AvroFileSystemStrategy<OrderAvro>("OrderJob");
            AvroFileSystemStrategy
                    .findWithHandler(OrderAvro.getClassSchema(), new File("order-archive-0625.avro"), 
                    (OrderAvro order) -> {
                        if (order.getShipping() > 4800) {
                            System.out.println("Matched shipping > 4800: id " + order.getOrderId() + " shipping " + order.getShipping());
                        }
            });

        } catch (Exception e) {}
    }    */



     
}
