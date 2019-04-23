package com.jiang;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.Properties;
import java.util.Set;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        Properties properties = System.getProperties();
        Set<String> strings = properties.stringPropertyNames();
        for (String propName : strings) {
            System.out.println(propName + "---->" + properties.getProperty(propName));
        }
    }
}
