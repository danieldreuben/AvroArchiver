package com.ross.serializer;

import com.ross.serializer.stategy.InMemoryArchiverStrategy;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryArchiverStrategyTest {

    @Test
    void testWriteAndReadOrderAvro() throws Exception {
        // Use our in-memory strategy
        InMemoryArchiverStrategy strategy = new InMemoryArchiverStrategy();

        // Build test record
        ItemAvro item1 = new ItemAvro("sku-1", 2, 9.99);
        ItemAvro item2 = new ItemAvro("sku-2", 1, 19.99);

        OrderAvro order = new OrderAvro(
                "order-123",
                5.50,
                ByteBuffer.wrap(new byte[]{1, 2, 3}),
                Arrays.asList(item1, item2)
        );

        List<OrderAvro> input = List.of(order);
        Schema schema = OrderAvro.getClassSchema();

        // --- Write into memory
        strategy.write(schema, () -> input);

        // --- Read back and verify
        strategy.read(schema, rec -> {
            OrderAvro o = (OrderAvro) rec;
            assertEquals(order.getOrderId().toString(), o.getOrderId().toString());
            assertEquals(order.getShipping(), o.getShipping());
            assertEquals(order.getItems().size(), o.getItems().size());
            return true;
        });
    }
    
    @Test
    void testFindMatchingRecords() throws Exception {
        InMemoryArchiverStrategy strategy = new InMemoryArchiverStrategy();

        // Build test record
        ItemAvro item1 = new ItemAvro("sku-1", 2, 9.99);
        ItemAvro item2 = new ItemAvro("sku-2", 1, 19.99);

        OrderAvro order1 = new OrderAvro(
                "order-123",
                5.50,
                ByteBuffer.wrap(new byte[]{1, 2, 3}),
                Arrays.asList(item1, item2)
        );

        OrderAvro order2 = new OrderAvro(
                "order-456",
                7.99,
                ByteBuffer.wrap(new byte[]{4, 5, 6}),
                List.of(item1)
        );

        List<OrderAvro> input = List.of(order1, order2);
        Schema schema = OrderAvro.getClassSchema();

        // Write to the in-memory store
        strategy.write(schema, () -> input);

        // Track whether a match was found
        AtomicBoolean found = new AtomicBoolean(false);

        // Use findMatchingRecords to locate order-456
        strategy.findMatchingRecords(
                OrderAvro.class,
                o -> "order-456".equals(o.getOrderId().toString()), // filter
                o -> {
                    found.set(true);
                    // verify fields
                    org.junit.jupiter.api.Assertions.assertEquals("order-456", o.getOrderId().toString());
                    org.junit.jupiter.api.Assertions.assertEquals(7.99, o.getShipping());
                    return false; // stop after first match
                }
        );

        assertTrue(found.get(), "Expected to find order-456");
    }    
}
