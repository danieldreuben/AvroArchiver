package com.ross.serializer.stategy;

    public class MatchResult {
        private final String index;
        private final String location;

        public MatchResult(String index, String location) {
            this.index = index;
            this.location = location;
        }

        public String getIndex() {
            return index;
        }

        public String getLocation() {
            return location;
        }

        @Override
        public String toString() {
            return "MatchResult{index='" + index + "', location='" + location + "'}";
        }
    }
