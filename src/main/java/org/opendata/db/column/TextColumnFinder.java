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
import java.math.BigDecimal;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.io.FileListReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.similarity.Support;
import org.opendata.core.profiling.datatype.DefaultDataTypeAnnotator;

/**
 * Identify columns that contain text values. Takes a list of column files as
 * input. Reads all values in the column and determines the fraction of text
 * values (from the distinct list of values). Outputs the absolute column file
 * path if the fraction of text values satisfies a given threshold constraint.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TextColumnFinder {
    
    /**
     * Output column files that have a fraction of text values that satisfies
     * the given constraint.
     * 
     * @param files
     * @param threshold
     * @param out
     * @throws java.io.IOException 
     */
    public void run(List<File> files, Threshold threshold, PrintWriter out) throws java.io.IOException {
        
        DefaultDataTypeAnnotator typeCheck = new DefaultDataTypeAnnotator();
        
        for (File file : files) {
            try (FlexibleColumnReader reader = new FlexibleColumnReader(file)) {
                int textCount = 0;
                int valueCount = 0;
                while (reader.hasNext()) {
                    if (typeCheck.getType(reader.next().getText()).isText()) {
                        textCount++;
                    }
                    valueCount++;
                }
                if (valueCount > 0) {
                    BigDecimal frac = new Support(textCount, valueCount).value();
                    if (threshold.isSatisfied(frac)) {
                        out.println(file.getAbsolutePath());
                    }
                }
            } catch (java.lang.NumberFormatException ex) {
                LOGGER.log(Level.SEVERE, file.getName(), ex);
                System.exit(-1);
            }
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <column-file-or-dir>\n" +
            "  <threshold-constraint>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(TextColumnFinder.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inFile = new File(args[0]);
        Threshold threshold = Threshold.getConstraint(args[1]);
        File outputFile = new File(args[2]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new TextColumnFinder().run(
                    new FileListReader(".txt").listFiles(inFile),
                    threshold,
                    out
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
