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
import org.opendata.curation.d4.Constants;
import org.opendata.core.util.Avg;

/**
 * Statistics computer for a set of expanded columns. Computes the number of
 * columns that were expanded, the average number of nodes in the expansion,
 * and the column with the largest expansion set.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExpandedColumnStats implements ExpandedColumnConsumer {

    private int _columnCount = 0;
    private int _expandedCount = 0;
    private int _maxExpansion = 0;
    private long _totalColumnNodes = 0;
    private long _totalExpandedNodes = 0;
    
    @Override
    public void close() {

    }

    @Override
    public void consume(ExpandedColumn column) {

        _columnCount++;
        _totalColumnNodes += column.originalNodes().length();
        
        int expansionSize = column.expansionSize();
        if (expansionSize > 0) {
            _expandedCount++;
            _totalExpandedNodes += (long)expansionSize;
            if (expansionSize > _maxExpansion) {
                _maxExpansion = expansionSize;
            }
        }
    }

    @Override
    public void open() {

        _columnCount = 0;
        _expandedCount = 0;
        _maxExpansion = 0;
        _totalColumnNodes = 0;
        _totalExpandedNodes = 0;
    }
    
    public void print(PrintWriter out) {

        out.println("NUMBER OF COLUMNS: " + _columnCount);
        out.println("AVG. COLUMN SIZE : " + new Avg(_totalColumnNodes,(long) _columnCount));
        out.println("EXPANDED COLUMNS : " + _expandedCount);
        out.println("MAX. EXPANSION   : " + _maxExpansion);
        if (_expandedCount > 0) {
            Avg avg = new Avg(_totalExpandedNodes, (long)_expandedCount, 2);
            out.println("AVG. EXPANSION   : " + avg);
        }
    }
    
    public void print() {

        System.out.println("NUMBER OF COLUMNS: " + _columnCount);
        System.out.println("AVG. COLUMN SIZE : " + new Avg(_totalColumnNodes,(long) _columnCount));
        System.out.println("EXPANDED COLUMNS : " + _expandedCount);
        System.out.println("MAX. EXPANSION   : " + _maxExpansion);
        if (_expandedCount > 0) {
            Avg avg = new Avg(_totalExpandedNodes, (long)_expandedCount, 2);
            System.out.println("AVG. EXPANSION   : " + avg);
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <column-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ExpandedColumnStats.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Expanded Column Stats - Version (" + Constants.VERSION + ")\n");

        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File columnFile = new File(args[0]);
        
        ExpandedColumnStats consumer = new ExpandedColumnStats();
        
        try (PrintWriter out = new PrintWriter(System.out)) {
            new ExpandedColumnReader(columnFile).stream(consumer);
            consumer.print(out);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
