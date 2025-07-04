package com.ross.excel.serializer;

import java.util.ArrayList;
import java.util.stream.Collectors;

import com.ross.excel.serializer.archiver.AvroFileSystemStrategy;


public class OrderJob {

    public OrderJob() {} 

      void testWriteOrders()  {
        try {
            final int[] count = {0};        
            new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .write (
                    OrderAvro.getClassSchema(),
                    () -> {
                        return ++count[0] < 3 ? // simulates a batch write
                            Order.getSerializableOrders(5) : new ArrayList<>();
                    }
                );
        } catch (Exception e) {}
    } 

    void testReadOrders() {
        try {
            final int[] count = {0};

            new AvroFileSystemStrategy<OrderAvro>("OrderJob")
                .read(OrderAvro.getClassSchema(), order -> {
                    OrderAvro t = (OrderAvro) order;
                    Order o = new Order(
                        t.getOrderId().toString(),
                        t.getShipping(),
                        t.getImageData() != null ? t.getImageData().array() : null,
                        t.getItems().stream()
                            .map(itemAvro -> new Item(
                                itemAvro.getSku().toString(),
                                itemAvro.getQuantity(),
                                itemAvro.getPrice()
                            ))
                            .collect(Collectors.toList())
                    );

                    System.out.println("Order (POC style): " + o);
                    return ++count[0] < 5; // stop after reading N..
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

