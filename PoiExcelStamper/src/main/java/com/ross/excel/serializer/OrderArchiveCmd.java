package com.ross.excel.serializer;

import com.ross.excel.serializer.mapper.AvroArchiveCmd;
import com.ross.excel.serializer.mapper.ArchiveNameResolver.ArchiveSchedule;

public class OrderArchiveCmd extends AvroArchiveCmd {
 
    public OrderArchiveCmd() {
        super(
            new OrderAvroMapper(), "order-archive", "/", ArchiveSchedule.MONTHLY
        );        
    }
}
