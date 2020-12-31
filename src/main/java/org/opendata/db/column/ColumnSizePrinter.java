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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.object.IdentifiableInteger;
import org.opendata.core.util.IdentifiableCounterSet;
import org.opendata.db.eq.CompressedTermIndex;
import org.opendata.db.eq.CompressedTermIndexFile;
import org.opendata.db.eq.EQ;

/**
 * Print number of terms for each column in a dataset.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnSizePrinter {
    
    /**
     * Output number of total values and distinct values per column.
     * 
     * @param eqIndex
     * @throws java.io.IOException 
     */
    public void run(CompressedTermIndex eqIndex) throws java.io.IOException {
        
        IdentifiableCounterSet columns = new IdentifiableCounterSet();
        
        for (EQ eq : eqIndex) {
            for (IdentifiableInteger col : eq.columnFrequencies()) {
                columns.inc(col.id(), col.value());
            }
        }
        for (IdentifiableInteger col : columns.toSortedList(false)) {
            System.out.println(String.format("%d\t%d", col.id(), col.value()));
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnSizePrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        
        try {
            new ColumnSizePrinter().run(new CompressedTermIndexFile(eqFile));
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
