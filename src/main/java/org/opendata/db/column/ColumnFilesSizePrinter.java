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
package org.opendata.db.column;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileListReader;
import org.opendata.core.io.FileSystem;

/**
 * Print number of distinct values and total values in a collection of database
 * column files.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFilesSizePrinter {
    
    /**
     * Output number of total values and distinct values per column.
     * 
     * @param files
     * @param out
     * @throws java.io.IOException 
     */
    public void run(List<File> files, PrintWriter out) throws java.io.IOException {
        
        System.out.println(files.size() + " COLUMNS");
        
        for (File file : files) {
            try (FlexibleColumnReader reader = new FlexibleColumnReader(file)) {
                int cellCount = 0;
                int valueCount = 0;
                while (reader.hasNext()) {
                    cellCount += reader.next().getCount();
                    valueCount++;
                }
                String name = file.getName();
                name = name.substring(0, name.length() - 7);
                int pos = name.indexOf(".");
                String line = name.substring(0, pos) + "\t" + name.substring(pos + 1);
                line += "\t" + valueCount + "\t" + cellCount;
                out.println(line);
                System.out.println(line);
            } catch (java.lang.NumberFormatException ex) {
                LOGGER.log(Level.SEVERE, file.getName(), ex);
                System.exit(-1);
            }
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <column-file-or-dir>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnFilesSizePrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inFile = new File(args[0]);
        File outputFile = new File(args[1]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new ColumnFilesSizePrinter().run(
                    new FileListReader(".txt").listFiles(inFile),
                    out
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
