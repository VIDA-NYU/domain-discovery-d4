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
package org.opendata.curation.d4.domain;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;

/**
 * Merge multiple local domain files into one output file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LocalDomainMerger {
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <output-file>\n" +
            "  <input-file-1>\n" +
            "  ...";
    
    private final static Logger LOGGER = Logger
            .getLogger(LocalDomainMerger.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length < 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File outputFile = new File(args[0]);
        
        HashMap<String, HashIDSet> domainIndex = new HashMap<>();
        try {
            for (int iArg = 1; iArg < args.length; iArg++) {
                File inputFile = new File(args[iArg]);
                try (BufferedReader in = FileSystem.openReader(inputFile)) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        String[] tokens = line.split("\t");
                        String key = tokens[1];
                        HashIDSet columns = new HashIDSet(tokens[2].split(","));
                        if (domainIndex.containsKey(key)) {
                           domainIndex.get(key).add(columns);
                        } else {
                            domainIndex.put(key, columns);
                        }
                    }
                }
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "MERGE", ex);
            System.exit(-1);
        }
        
        int domainId = 0;
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            for (String key : domainIndex.keySet()) {
                HashIDSet columns = domainIndex.get(key);
                out.println((domainId++) + "\t" + key + "\t" + columns.toIntString());
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);            
        }
    }
}
