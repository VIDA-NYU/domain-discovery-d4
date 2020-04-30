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
package org.opendata.curation.d4.column;

import java.io.File;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.curation.d4.Constants;

/**
 * Write number of equivalence classes in original column and in the
 * expansion set.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnExpansionStatsWriter implements ExpandedColumnConsumer {

    private final PrintWriter _out;
    
    public ColumnExpansionStatsWriter(PrintWriter out) {
        
        _out = out;
    }
    
    @Override
    public void close() {

    }

    @Override
    public void consume(ExpandedColumn column) {

        int colId = column.id();
        int origSize = column.originalNodes().length();
        int expSize = column.expansionSize();
        
        _out.println(colId + "\t" + origSize + "\t" + expSize);
    }

    @Override
    public void open() {

    }


    private static final String COMMAND =
            "Usage:\n" +
            "  <column-file>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnExpansionStatsWriter.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Expanded Column Size Writer - Version (" + Constants.VERSION + ")\n");

        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File columnFile = new File(args[0]);
        File outputFile = new File(args[1]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new ExpandedColumnReader(columnFile)
                    .stream(new ColumnExpansionStatsWriter(out));
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
