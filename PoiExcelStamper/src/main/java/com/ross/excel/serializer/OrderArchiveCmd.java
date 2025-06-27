package com.ross.excel.serializer;

import com.ross.excel.serializer.avro.AvroArchiveCmd;
import com.ross.excel.serializer.archiver.ArchiveNameResolver.ArchiveSchedule;

public class OrderArchiveCmd<T> extends AvroArchiveCmd<OrderAvro>  {
 
    public OrderArchiveCmd() {
        super(
            new OrderAvroMapper(), "order-archive", "/", ArchiveSchedule.MONTHLY
        );        
    }
}
