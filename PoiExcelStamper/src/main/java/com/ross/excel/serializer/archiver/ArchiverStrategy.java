package com.ross.excel.serializer.archiver;

import java.io.IOException;

public interface ArchiverStrategy {
    void serialize() throws IOException;
    void deserialize() throws IOException;

}