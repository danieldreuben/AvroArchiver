package com.ross.excel.serializer.archiver;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ArchiveNameResolver {

    public enum ArchiveSchedule {
        YEARLY,
        MONTHLY,
        DAILY,
        HOURLY
    }

    public static String archiveNamingResolver(String prefix, ArchiveSchedule granularity, ZonedDateTime dateTime) {
        String suffix;

        switch (granularity) {
            case YEARLY:
                suffix = dateTime.format(DateTimeFormatter.ofPattern("uuuu"));
                break;
            case MONTHLY:
                suffix = dateTime.format(DateTimeFormatter.ofPattern("MMuu"));
                break;
            case DAILY:
                suffix = dateTime.format(DateTimeFormatter.ofPattern("MMdduu"));
                break;
            case HOURLY:
                suffix = dateTime.format(DateTimeFormatter.ofPattern("MMdduu-HH"));
                break;
            default:
                throw new IllegalArgumentException("Unsupported granularity: " + granularity);
        }

        return prefix + "-" + suffix + ".avro";
    }

    // Overload for convenience
    public static String resolveAvroArchiveFileName(String baseName, ArchiveSchedule schedule) {
        return archiveNamingResolver(baseName, schedule, ZonedDateTime.now());
    }
}
