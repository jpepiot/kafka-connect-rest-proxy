package com.athena.kafka.connect.restproxy.common;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

import static java.nio.file.Files.newInputStream;

@Slf4j
@UtilityClass
public class VersionUtils {

    private static final String DEFAULT_VERSION = "0.0.0.0";

    public static String getVersion() {
        try (InputStream input = openFile("version.properties")) {
            Properties properties = new Properties();
            properties.load(input);
            return properties.getProperty("version", DEFAULT_VERSION);
        } catch (Exception ex) {
            log.warn("Error loading version.properties, default version " + DEFAULT_VERSION + " will be used");
            return DEFAULT_VERSION;
        }
    }

    private static InputStream openFile(String fileName) throws IOException {
        InputStream stream = VersionUtils.class.getClassLoader().getResourceAsStream(fileName);
        return stream != null ? stream : newInputStream(Paths.get(fileName));
    }
}
