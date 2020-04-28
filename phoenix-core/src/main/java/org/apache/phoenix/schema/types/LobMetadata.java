package org.apache.phoenix.schema.types;

public class LobMetadata {
    public LobMetadata(long size, String lobLocator) {
        this.size = size;
        this.lobLocator = lobLocator;
    }

    private final long size; //size in bytes
    private final String lobLocator;

    public long getSize() {
        return size;
    }

    public String getLobLocator() {
        return lobLocator;
    }
}
