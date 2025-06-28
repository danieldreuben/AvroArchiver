package com.ross.excel.serializer;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Autowired;

import com.ross.excel.serializer.archiver.ArchiveNameResolver.ArchiveSchedule;
import com.ross.excel.serializer.avro.ArchiveCommand;
import com.ross.excel.serializer.avro.AvroFileSystemStrategy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.util.List;


@SpringBootTest
@TestMethodOrder(OrderAnnotation.class)
class AvroApplicationArchiverTests {

	@Autowired
    //private AvroAchiver archiverApp;

	@Test
	void contextLoads() {
	}

    @Test
    void testDeserializeOrders()  {
        try {
            new AvroFileSystemStrategy<OrderAvro>()
                //new ArchiveCommand<>("test","test",ArchiveSchedule.HOURLY) new ArchiveCommand<>()
                    .deserialize3(
                        OrderAvro.getClassSchema(),
                        new File("order-archive-0625.avro"),
                        10,
                        (List<OrderAvro> orders) -> {
                            orders.forEach(order -> System.out.println(order.getOrderId()));
                        }
                    );

        } catch (Exception e) {}
    }    

    @Test
    void testSerializerOrders() {
        try {
            OrderHelper helper = new OrderHelper();
            new AvroFileSystemStrategy<OrderAvro>(
                ).serializeBatched (
                    OrderAvro.getClassSchema(),
                    new File("order-archive-0625.avro"),
                    10,
                    () -> {
                        System.out.println("getting records for archive..");
                        return helper.getRecordsToArchive();

                    }
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testFindOrders()  {
        try {
            new AvroFileSystemStrategy<OrderAvro>();
            AvroFileSystemStrategy
                    .findWithHandler(OrderAvro.getClassSchema(), new File("order-archive-0625.avro"), 
                    (OrderAvro order) -> {
                        if (order.getShipping() > 4000) {
                            System.out.println("Matched shipping Order: " + order.getOrderId() + " shipping " + order.getShipping());
                        }
            });

        } catch (Exception e) {}
    }    

	/*@Test 
	void testWriteArchiveOrders() {
        try {
            new AvroFileSystemStrategy<OrderAvro> (
                new OrderArchiveCmd<OrderAvro>()
            ).serialize();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}*/

    /*@Test 
	void testReadArchivedOrders() {
        try {
            new AvroFileSystemStrategy<OrderAvro>(
                new OrderArchiveCmd<OrderAvro>()
            ).deserialize();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}   */
     
}
