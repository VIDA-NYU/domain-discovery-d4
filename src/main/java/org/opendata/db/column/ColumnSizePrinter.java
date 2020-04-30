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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.db.Database;
import org.opendata.db.eq.EQIndex;

/**
 * Print number of equivalence classes and terms in columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnSizePrinter {
    
    /**
     * Output number of equivalence classes and terms per column.
     * 
     * @param eqIndex
     * @param out
     * @throws java.io.IOException 
     */
    public void run(EQIndex eqIndex, PrintWriter out) throws java.io.IOException {
        
        Database db = new Database(eqIndex);
        
        for (Column column : db.columns()) {
            int termCount = 0;
            for (int nodeId : column) {
                termCount += eqIndex.get(nodeId).termCount();
            }
            out.println(column.id() + "\t" + column.length() + "\t" + termCount);
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnSizePrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File outputFile = new File(args[1]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new ColumnSizePrinter().run(new EQIndex(eqFile), out);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
