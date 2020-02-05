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

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.Constants;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.util.count.Counter;

/**
 * Write load files for the datasets and columns table.
 * 
 * The datasets table contains the unique dataset identifier, name, number of
 * column and number of rows.
 * 
 * The columns table contains the unique column identifier, name, dataset
 * identifier and number of distinct terms.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatasetsAndColumnsLoadFileGenerator {
   
    private static final Logger LOGGER = Logger.getGlobal();
    
    public void run(
            File datasetFile,
            File datasetDir,
            File columnsFile,
            File textColumnsFile,
            String prefix,
            File outputDir
    ) throws java.io.IOException {
        
        HashIDSet columnFilter = new HashIDSet();
        try (BufferedReader in = FileSystem.openReader(textColumnsFile)) {
            String line;
            while ((line = in.readLine()) != null) {
               int colId = Integer.parseInt(new File(line).getName().split("\\.")[0]);
                columnFilter.add(colId);
            }
        }
        
        DatabaseLoadFileWriter writer = new DatabaseLoadFileWriter();
        
        File scriptFile = FileSystem.joinPath(outputDir, "load-datasets.sql");
        
        FileSystem.createParentFolder(scriptFile);

        try (PrintWriter script = FileSystem.openPrintWriter(scriptFile)) {
            HashMap<String, Counter> datasetStats = writer.writeColumnsFile(columnsFile,
                    columnFilter,
                    prefix + "columns",
                    FileSystem.joinPath(outputDir, "columns.del"),
                    script
            );
            writer.writeDatasetFile(datasetFile,
                    datasetDir,
                    datasetStats,
                    prefix + "datasets",
                    FileSystem.joinPath(outputDir, "datasets.del"),
                    script
            );
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <dataset-file>\n" +
            "  <dataset-dir>\n" +
            "  <columns-file>\n" +
            "  <text-columns-file>\n" +
            "  <prefix>\n" +
            "  <output-directory>";
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Datasets and Columns Load File Generator - Version (" + Constants.VERSION + ")\n");

        if (args.length != 6) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File datasetFile = new File(args[0]);
        File datasetDir = new File(args[1]);
        File columnsFile = new File(args[2]);
        File textColumnsFile = new File(args[3]);
        String prefix = args[4];
        File outputDir = new File(args[5]);
        
        try {
            new DatasetsAndColumnsLoadFileGenerator().run(
                    datasetFile,
                    datasetDir,
                    columnsFile,
                    textColumnsFile,
                    prefix,
                    outputDir
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
