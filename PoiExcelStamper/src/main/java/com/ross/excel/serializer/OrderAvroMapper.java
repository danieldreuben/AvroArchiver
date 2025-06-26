package com.ross.excel.serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import com.ross.excel.serializer.mapper.AvroDataMapper;

public class OrderAvroMapper implements AvroDataMapper {

    public OrderAvroMapper() {
    }

    @Override
    public Schema getSchema() {
        return OrderAvro.getClassSchema();
    }

    @Override
    public List<SpecificRecord> getRecords() {
        List<SpecificRecord> records = new ArrayList<>();

        for (int i = 1; i <= 5; i++) {
            String orderId = "ORDER-" + String.format("%03d", i);

            List<Item> items = List.of(
                new Item("SKU-" + i + "-A", i % 5 + 1, 10.0 + i),
                new Item("SKU-" + i + "-B", i % 3 + 1, 5.5 + i)
            );

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
}

