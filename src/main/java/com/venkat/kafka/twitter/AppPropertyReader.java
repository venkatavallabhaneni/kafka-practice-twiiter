package com.venkat.kafka.twitter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppPropertyReader {

    private static Properties properties;

    static {

        properties = new Properties();

        try (InputStream input = AppPropertyReader.class.getClassLoader().getResourceAsStream("application.properties.loc")) {
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static String getValue(String key) {

        return properties.getProperty(key);
    }

}