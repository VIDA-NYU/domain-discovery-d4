/*
 * This file is part of the Data-Driven Domain Discovery Tool (D4).
 * 
 * Copyright (c) 2018-2020 New York University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opendata.curation.d4.export;

import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;

/**
 * Write primary block (domain) from strong domain JSON file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class PrimaryDomainWriter {
    
    public void run(File inputDir, File outputDir) throws java.io.IOException {
        
        FileSystem.createFolder(outputDir);
        
        for (File file : inputDir.listFiles()) {
            if (file.getName().endsWith(".json")) {
                String domainId = file
                        .getName()
                        .substring(0, file.getName().indexOf("."));
                String domainName = null;
                int columnCount = 0;
                List<String> terms = new ArrayList<>();
                try (JsonReader reader = new JsonReader(new FileReader(file))) {
                    reader.beginObject();
                    while (reader.hasNext()) {
                        String key = reader.nextName();
                        switch(key) {
                            case "name":
                                domainName = reader
                                        .nextString()
                                        .replaceAll(" ", "_");
                                break;
                            case "columns":
                                reader.beginArray();
                                while (reader.hasNext()) {
                                    columnCount++;
                                    reader.skipValue();
                                }
                                reader.endArray();
                                break;
                            case "terms":
                                reader.beginArray();
                                reader.beginArray();
                                while (reader.hasNext()) {
                                    reader.beginObject();
                                    while (reader.hasNext()) {
                                        if (reader.nextName().equals("name")) {
                                            terms.add(reader.nextString());
                                        } else {
                                            reader.skipValue();
                                        }
                                    }
                                    reader.endObject();
                                }
                                reader.endArray();
                                while (reader.hasNext()) {
                                    reader.skipValue();
                                }
                                reader.endArray();
                                break;
                            default:
                                reader.skipValue();
                                break;
                        }
                    }
                    reader.endObject();
                }
                Collections.sort(terms);
                String filename = String.format(
                        "%s-%d-%s.txt",
                        domainId,
                        columnCount,
                        domainName
                );
                File outFile = FileSystem.joinPath(outputDir, filename);
                try (PrintWriter out = FileSystem.openPrintWriter(outFile)) {
                    for (String term : terms) {
                        out.println(term);
                    }
                }
            }
        }
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <input-dir>\n" +
            "  <output-dir>";
    
    private final static Logger LOGGER = Logger
            .getLogger(PrimaryDomainWriter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputDir = new File(args[0]);
        File outputDir = new File(args[1]);
        
        try {
            new PrimaryDomainWriter().run(inputDir, outputDir);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
