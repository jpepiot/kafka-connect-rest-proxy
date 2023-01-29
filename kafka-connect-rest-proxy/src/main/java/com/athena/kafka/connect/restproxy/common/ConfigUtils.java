package com.athena.kafka.connect.restproxy.common;

import lombok.experimental.UtilityClass;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.*;
import static java.util.stream.IntStream.rangeClosed;

@UtilityClass
public class ConfigUtils {

    public static Map<String, List<String>> breakDownHeaders(String headers) {
        return breakDownMultiValuePairs(headers, "(?<!\\\\),", ":")
                .entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, entry ->
                        entry.getValue().stream().map(value ->
                                value.replaceAll("\\\\,", ","))
                                .collect(toList())));
    }

    public static Map<String, List<String>> breakDownQueryParams(String queryParams) {
        return breakDownMultiValuePairs(queryParams, "&", "=");
    }

    public static Map<String, String> breakDownMap(String mapString) {
        return breakDownPairs(mapString, ",", "=");
    }

    public static List<Map<String, String>> breakDownMapList(String mapList) {
        return breakDownList(mapList, ";")
                .map(ConfigUtils::breakDownMap)
                .collect(toList());
    }

    private static Map<String, String> breakDownPairs(String itemLine, String itemSplitter, String pairSplitter) {
        return breakDownPairs(itemLine, itemSplitter, pairSplitter, toMap(Entry::getKey, Entry::getValue));
    }

    private static Map<String, List<String>> breakDownMultiValuePairs(String itemLine, String itemSplitter, String pairSplitter) {
        return breakDownPairs(itemLine, itemSplitter, pairSplitter, groupingBy(Entry::getKey, mapping(Entry::getValue, toList())));
    }

    private static <T> Map<String, T> breakDownPairs(String itemList, String itemSplitter, String pairSplitter, Collector<Entry<String, String>, ?, Map<String, T>> collector) {
        return breakDownList(itemList, itemSplitter)
                .map(headerLine -> breakDownPair(headerLine, pairSplitter))
                .collect(collector);
    }

    private static Entry<String, String> breakDownPair(String pairLine, String pairSplitter) {
        String[] parts = pairLine.split(pairSplitter, 2);
        if (parts.length < 2) {
            throw new IllegalStateException("Incomplete pair: " + pairLine);
        }
        return new SimpleEntry<>(parts[0].trim(), parts[1].trim());
    }

    public static List<String> breakDownList(String itemList) {
        return breakDownList(itemList, ",")
                .collect(toList());
    }

    private static Stream<String> breakDownList(String itemList, String splitter) {
        if (itemList==null || itemList.length()==0) {
            return Stream.empty();
        }
        return Stream.of(itemList.split(splitter))
                .map(String::trim)
                .filter(it -> !it.isEmpty());
    }

    public static Set<Integer> parseIntegerRangedList(String rangedList) {
        return breakDownList(rangedList, ",")
                .map(ConfigUtils::parseIntegerRange)
                .flatMap(Set::stream)
                .collect(toSet());
    }

    private static Set<Integer> parseIntegerRange(String range) {
        String[] rangeString = range.split("\\.\\.");
        if (rangeString.length==0 || rangeString[0].length()==0) {
            return emptySet();
        } else if (rangeString.length==1) {
            return asSet(Integer.valueOf(rangeString[0].trim()));
        } else if (rangeString.length==2) {
            int from = Integer.parseInt(rangeString[0].trim());
            int to = Integer.parseInt(rangeString[1].trim());
            return (from < to ? rangeClosed(from, to):rangeClosed(to, from)).boxed().collect(toSet());
        }
        throw new IllegalStateException(String.format("Invalid range definition %s", range));
    }

    private static Set<Integer> asSet(Integer... values) {
        return Stream.of(values).collect(toSet());
    }
}
