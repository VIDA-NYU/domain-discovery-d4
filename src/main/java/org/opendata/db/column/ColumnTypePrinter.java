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
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileListReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.profiling.datatype.DefaultDataTypeAnnotator;
import org.opendata.core.profiling.datatype.label.DataType;
import org.opendata.core.util.count.Counter;

/**
 * Print summary of data types for all values in a given list of columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnTypePrinter {
    
    private static final int[] DATATYPES = new int[]{
        DataType.INTEGER,
        DataType.LONG,
        DataType.DECIMAL,
        DataType.DATE,
        DataType.GEO,
        DataType.TEXT
    };
    
    /**
     * Output column files that have a fraction of text values that satisfies
     * the given constraint.
     * 
     * @param files
     * @param limitCount
     * @param out
     * @throws java.io.IOException 
     */
    public void run(List<File> files, int limitCount, PrintWriter out) throws java.io.IOException {
        
        DefaultDataTypeAnnotator typeCheck = new DefaultDataTypeAnnotator();
        
        for (File file : files) {
            try (FlexibleColumnReader reader = new FlexibleColumnReader(file)) {
                HashMap<Integer, Counter> types = new HashMap<>();
                int valueCount = 0;
                while (reader.hasNext()) {
                    DataType type = typeCheck.getType(reader.next().getText());
                    int key = type.id();
                    if (!types.containsKey(key)) {
                        types.put(key, new Counter(1));
                    } else {
                        types.get(key).inc();
                    }
                    valueCount++;
                    if ((limitCount > 0) && (valueCount >= limitCount)) {
                        break;
                    }
                }
                String line = Integer.toString(reader.columnId());
                for (int key : DATATYPES) {
                    int count = 0;
                    if (types.containsKey(key)) {
                        count = types.get(key).value();
                    }
                    line += "\t" + count;
                }
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
            "  <limit-value-count-per-column> [-1 for all values]\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnTypePrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inFile = new File(args[0]);
        int limitCount = Integer.parseInt(args[1]);
        File outputFile = new File(args[2]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new ColumnTypePrinter().run(
                    new FileListReader(".txt").listFiles(inFile),
                    limitCount,
                    out
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
