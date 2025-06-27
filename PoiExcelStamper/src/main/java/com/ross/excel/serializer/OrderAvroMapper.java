package com.ross.excel.serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import com.ross.excel.serializer.avro.AvroDataMapper;

public class OrderAvroMapper implements AvroDataMapper<OrderAvro>  {
    private int callCount = 0;
    private final int maxOrders = 5;

    public OrderAvroMapper() {
    }

    @Override
    public Schema getSchema() {
        return OrderAvro.getClassSchema();
    }

    @Override
    public List<OrderAvro> getRecordsToArchive() {

        int randomorderbatch = new Random().nextInt(5) + 1;
        
        return ++callCount < maxOrders ? getAvroSchemaRecords(randomorderbatch) : new ArrayList<>();
    }

    private List<OrderAvro> getAvroSchemaRecords(int num) {
            
        List<OrderAvro> records = new ArrayList<>();

        for (int i = 0; i <= num; i++) {
            
            int randomordernum = new Random().nextInt(10000) + 1;
            String orderId = "ORDER-" + String.format("%03d", randomordernum);

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
                .setItems(itemAvros)
                .build();

            records.add(orderAvro);
        }

        return records;
    }

    @Override
    public void setRecordsFromArchive(List<OrderAvro> t) {

        List<OrderAvro> orderList;

        throw new UnsupportedOperationException("Unimplemented method 'setRecordsFromArchive'");
    }
}

