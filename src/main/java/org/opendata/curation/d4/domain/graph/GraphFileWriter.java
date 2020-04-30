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
package org.opendata.curation.d4.domain.graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.util.MemUsagePrinter;
import org.opendata.curation.d4.Constants;

/**
 * Sort edges in a graph file for a expanded column set and write them to file.
 * Create on file per column in a given output directory.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class GraphFileWriter {
    
    public void run(File inputFile, File outputDir, boolean verbose) throws java.io.IOException {
        
        FileSystem.createFolder(outputDir);
        
        HashMap<Integer, List<String>> columns = new HashMap<>();
        
        if (verbose) {
            System.out.println("START READING EDGE FILE @ " + new Date());
        }
        
        try (BufferedReader in = FileSystem.openReader(inputFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                int columnId = Integer.parseInt(tokens[0]);
                if (!columns.containsKey(columnId)) {
                    columns.put(columnId, new ArrayList<>());
                }
                columns.get(columnId).add(tokens[1] + "\t" + tokens[2]);
            }
        }
        
        if (verbose) {
            System.out.println("START WRITING GRAPH FILES @ " + new Date());
            new MemUsagePrinter().print();
        }

        for (int columnId : columns.keySet()) {
            String filename = columnId + ".txt.gz";
            File outputFile = FileSystem.joinPath(outputDir, filename);
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                for (String line : columns.get(columnId)) {
                    out.println(line);
                }
            }
        }
        
        if (verbose) {
            System.out.println("DONE WRITING GRAPH FILES @ " + new Date());
        }
    }
        
    public void run(File inputFile, File outputDir) throws java.io.IOException {
        
        this.run(inputFile, outputDir, true);
    }
    
    private final static String COMMAND = 
            "Usage:\n" +
            "  <input-file>\n" +
            "  <output-dir>";
    
    private final static Logger LOGGER = Logger
            .getLogger(GraphFileWriter.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Graph File Writer - Version (" + Constants.VERSION + ")\n");
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputFile = new File(args[0]);
        File outputDir = new File(args[1]);
        
        try {
            new GraphFileWriter().run(inputFile, outputDir);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
