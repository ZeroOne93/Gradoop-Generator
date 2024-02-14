package org.generator.factories;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class LoadVertex {

    /**
     * @return
     */
    public static String returnPathToPropertiesValues() {
        // return path to person properties as they are the only properties considered to this generator.
        URL url = Main.class.getClassLoader().getResource("Values/person/");
        if (url != null) {
            return url.getPath();
        } else {
            return "The properties file was not found "; // Or throw an exception if the resource must exist
        }

    }

    public static Map<String, ArrayList<String>> loadPropsWithValuesOptimized(String propValuesPath) {

        Map<String, ArrayList<String>> propertiesNamesAndValues = new HashMap<>();


        File folder = new File(propValuesPath);

        if (folder.isDirectory()) {
            File[] files = folder.listFiles();


            assert files != null;
            for (File file : files) {

                if (file.isFile()) {
                    ArrayList<String> propertyValues;
                    System.out.println("Reading file: " + file.getName());
                    if (file.getName().equals("age_values")) {
                        propertyValues = new ArrayList<>(60);
                    } else {
                        propertyValues = new ArrayList<>(1005);
                    }

                    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                        String line;
                        while ((line = reader.readLine()) != null) {

                            propertyValues.add(line);
                        }
                        propertyValues.trimToSize();
                        propertiesNamesAndValues.put(getFirstTokenOfFileName(file.getName(), "_"), propertyValues);

                        System.out.println("-------------------------------");
                    } catch (IOException e) {
                        System.out.println("There was an error reading property files");
                    }
                }

            }
        }

        return propertiesNamesAndValues;
    }

    public static String getFirstTokenOfFileName(String input, String delimiter) {
        String[] tokens = input.split(delimiter);
        if (tokens.length > 0) {
            return tokens[0];

        } else {
            System.out.println("No Tokens");
            return ""; // Return an empty string if no tokens found
        }
    }
}
