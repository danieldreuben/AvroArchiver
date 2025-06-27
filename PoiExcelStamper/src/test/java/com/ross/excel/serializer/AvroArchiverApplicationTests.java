package com.ross.excel.serializer;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Autowired;

import com.ross.excel.serializer.avro.AvroFileSystemStrategy;

import org.apache.avro.specific.SpecificRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestMethodOrder;

//import static org.junit.Assert.assertTrue;
//
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
	void testWriteArchiveOrders() {
        try {
            new AvroFileSystemStrategy<OrderAvro> (
                new OrderArchiveCmd<OrderAvro>()
            ).serialize();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

    @Test 
	void testReadArchivedOrders() {
        try {
            new AvroFileSystemStrategy<OrderAvro>(
                new OrderArchiveCmd<OrderAvro>()
            ).deserialize();

            //orders.stream().forEach(System.out::println);

        } catch (Exception e) {
            e.printStackTrace();
        }
	}   

}
