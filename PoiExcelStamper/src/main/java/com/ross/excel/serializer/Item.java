package com.ross.excel.serializer;

public class Item {
    private String sku;
    private int quantity;
    private double price;

    // Constructors
    public Item() {}

    public Item(String sku, int quantity, double price) {
        this.sku = sku;
        this.quantity = quantity;
        this.price = price;
    }

    // Getters and Setters
    public String getSku() { return sku; }
    public void setSku(String sku) { this.sku = sku; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getPrice() { return price; }
    public void setPrice(double price) { this.price = price; }

    @Override
    public String toString() {
        return "Item{" +
                "sku='" + sku + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                '}';
    }
}