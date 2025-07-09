package com.ross.excel.serializer;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class Order {
    private String orderId;
    private double shipping;
    private byte[] imageData; // Nullable
    private List<Item> items;

    // Constructors
    public Order() {}

    public Order(String orderId, double shipping, byte[] imageData, List<Item> items) {
        this.orderId = orderId;
        this.shipping = shipping;
        this.imageData = imageData;
        this.items = items;
    }

    // Getters and Setters
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public double getShipping() { return shipping; }
    public void setShipping(double shipping) { this.shipping = shipping; }

    public byte[] getImageData() { return imageData; }
    public void setImageData(byte[] imageData) { this.imageData = imageData; }

    public List<Item> getItems() { return items; }
    public void setItems(List<Item> items) { this.items = items; }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", shipping=" + shipping +
                ", imageData=" + (imageData != null ? imageData.length + " bytes" : "null") +
                ", items=" + items +
                '}';
    }

    public static List<OrderAvro> getAvroOrders(int num) {            
        List<OrderAvro> records = new ArrayList<>();
        byte[] imageBytes = null;

        for (int i = 0; i <= num; i++) {
            
            int randomordernum = new Random().nextInt(10000) + 1;
            String orderId = "ORDER-" + String.format("%03d", randomordernum);
            double shipping = new Random().nextInt(5000);

            try {              
                File imageFile = new File("arc.png");
                imageBytes = Files.readAllBytes(imageFile.toPath());        

            } catch (Exception e) { e.printStackTrace(); }

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
                    .build())
                .collect(Collectors.toList());

            // Build the Avro order
            OrderAvro orderAvro = OrderAvro.newBuilder()
                .setOrderId(orderId)
                .setShipping(shipping)
                //.setImageData(ByteBuffer.wrap(imageBytes))
                .setItems(itemAvros)
                .build();

            records.add(orderAvro);
        }
        return records;
    }

}

