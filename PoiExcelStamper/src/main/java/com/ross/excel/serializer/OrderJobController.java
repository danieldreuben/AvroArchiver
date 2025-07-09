package com.ross.excel.serializer;

import java.util.ArrayList;
import java.util.stream.Collectors;

import com.ross.excel.serializer.archiver.AvroFileSystemStrategy;


public class OrderJobController {

    public OrderJobController() {} 

    void testWriteOrders()  {
        try {
            final int[] count = {0};        
            new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .write (
                    OrderAvro.getClassSchema(),
                    () -> {
                        return ++count[0] < 5 ? // simulates a batch write
                            Order.getAvroOrders(5) : new ArrayList<>();
                    }
                );
        } catch (Exception e) {}
    } 

    void testReadOrders() {
        try {
            final int[] count = {0};

            new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .read(OrderAvro.getClassSchema(), order -> {
                    Order s = new Order();
                    s.setAvroOrder((OrderAvro) order);
                    System.out.println(s.toString());
                    return ++count[0] < 25; // stop after N..
                });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*public void setRecordsFromArchive(List<SpecificRecord> t) {

        List<OrderAvro> orderList = t.stream()
            .filter(OrderAvro.class::isInstance)
            .map(OrderAvro.class::cast)
            .collect(Collectors.toList());

        orderList.stream().forEach(System.out::println);
    }*/

}

