package com.ross.excel.serializer;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.avro.specific.SpecificRecord;


//public class OrderAvroMapper implements AvroDataMapper<OrderAvro>  {
public class OrderHelper {
    private int callCount = 0;
    private final int maxOrders = 5;

    public OrderHelper() {
    }

    /*@Override
    public Schema getSchema() {
        return OrderAvro.getClassSchema();
    }*/

    //@Override
    public void setRecordsFromArchive(List<SpecificRecord> t) {

        List<OrderAvro> orderList = t.stream()
            .filter(OrderAvro.class::isInstance)
            .map(OrderAvro.class::cast)
            .collect(Collectors.toList());

        orderList.stream().forEach(System.out::println);
    }

    //@Override
    public List<OrderAvro> getRecordsToArchive() {
        int randomorderbatch = new Random().nextInt(5) + 1;
        
        return ++callCount < maxOrders ? getSerializeOrders(randomorderbatch) : new ArrayList<>();
    }


    private List<OrderAvro> getSerializeOrders(int num) {
            
        List<OrderAvro> records = new ArrayList<>();

        for (int i = 0; i <= num; i++) {
            
            int randomordernum = new Random().nextInt(10000) + 1;
            String orderId = "ORDER-" + String.format("%03d", randomordernum);
            double shipping = new Random().nextInt(5000);
            
            byte[] imageBytes;
            try { 
                File imageFile = new File("test.png");
                imageBytes = Files.readAllBytes(imageFile.toPath());            
            } catch (Exception e) { e.printStackTrace();}

            List<Item> items = new ArrayList<Item>();
            int randomsku = new Random().nextInt(10000) + 1;
            int randomnumofitems = new Random().nextInt(5) + 1;

            for (int t = 0; t < randomnumofitems; t++) {
                int randomprice = new Random().nextInt(10000) + 1;
                int randomquantity = new Random().nextInt(10000) + 1;

                items.add(new Item("SKU-" + randomsku + "-A", randomquantity + 1, randomprice + 10));
            }

            // Convert to Avro item list
            List<ItemAvro> itemAvros = items.stream()
                .map(item -> ItemAvro.newBuilder()
                    .setSku(item.getSku())
                    .setQuantity(item.getQuantity())
                    .setPrice(item.getPrice())
                    //.setImage1("abc".getBytes())
                    .build())
                .collect(Collectors.toList());

            // Build the Avro order
            OrderAvro orderAvro = OrderAvro.newBuilder()
                .setOrderId(orderId)
                .setShipping(shipping)
                .setItems(itemAvros)
                .build();

            records.add(orderAvro);
        }

        return records;
    }

}

