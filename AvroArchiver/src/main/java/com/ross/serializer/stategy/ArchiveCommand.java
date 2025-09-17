package com.ross.serializer.stategy;

import java.util.List;

public interface ArchiveCommand {
    public abstract boolean put(String name);
	public abstract boolean get(String name);
	public abstract List<String> getNames(String ref);	
}
