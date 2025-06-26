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
	void testArchiveOrders() {
        try {
            new AvroFileSystemStrategy (
                new OrderArchiveCmd()
            ).archive();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

    @Test 
	void testReadArchivedOrders() {
        try {
            List<SpecificRecord> orders = new AvroFileSystemStrategy(
                new OrderArchiveCmd()
            ).read();

            orders.stream().forEach(System.out::println);

        } catch (Exception e) {
            e.printStackTrace();
        }
	}   

}
