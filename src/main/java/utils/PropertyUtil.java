package utils;

import application.DataStreamJob;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtil {
    private static final String CONFIG_FILE = "config.properties";

    private static Properties loadProperties() {
        Properties prop = new Properties();

        try (InputStream propsInput = DataStreamJob.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            prop.load(propsInput);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return prop;
    }

    public static Properties getProperties() {
        return loadProperties();
    }
}
