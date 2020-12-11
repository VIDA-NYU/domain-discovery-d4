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
package org.opendata.db.eq;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;

/**
 * Write histogram of column counts for all equivalence classes and terms.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQColumnCountHistorgramWriter {
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(EQColumnCountHistorgramWriter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File outFile = new File(args[1]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outFile)) {
            HashMap<Integer, int[]> histogram = new HashMap<>();
            int maxColCount = 0;
            EQIndex eqIndex = new EQIndex(eqFile);
            for (EQ eq : eqIndex) {
                int colCount = eq.columns().length();
                int termCount = eq.terms().length();
                if (histogram.containsKey(colCount)) {
                    int[] entry = histogram.get(colCount);
                    entry[0]++;
                    entry[1] += termCount;
                } else {
                    histogram.put(colCount, new int[]{1, termCount});
                }
                if (colCount > maxColCount) {
                    maxColCount = colCount;
                }
            }
            for (int iCount = 1; iCount <= maxColCount; iCount++) {
                int[] counts;
                if (histogram.containsKey(iCount)) {
                    counts = histogram.get(iCount);
                } else {
                    counts = new int[]{0, 0};
                }
                out.println(iCount + "\t" + counts[0] + "\t" + counts[1]);
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
