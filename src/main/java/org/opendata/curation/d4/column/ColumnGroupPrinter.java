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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

/**
 * Write expanded column lists.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnGroupPrinter {
    
    public void run(
            IdentifiableObjectSet<Column> db,
            int fileCount,
            String outputPrefix
    ) throws java.io.IOException {
        
        HashMap<String, ExpandedColumn> columnIndex = new HashMap<>();
        HashMap<Integer, HashIDSet> groups = new HashMap<>();
        HashMap<String, Integer> mapping = new HashMap<>();
        for (int columnId : db.keys()) {
            Column column = db.get(columnId);
            String key = column.toIntString();
            if (!columnIndex.containsKey(key)) {
                columnIndex.put(key, new MutableExpandedColumn(column));
            } else {
                if (!mapping.containsKey(key)) {
                    int colId = columnIndex.get(key).id();
                    HashIDSet columnSet = new HashIDSet();
                    columnSet.add(colId);
                    columnSet.add(column.id());
                    groups.put(colId, columnSet);
                    mapping.put(key, colId);
                } else {
                    groups.get(mapping.get(key)).add(column.id());
                }
            }
        }
        
        // Sort column in decreasing number of nodes
        List<ExpandedColumn> columns;
        columns = new ArrayList<>(columnIndex.values());
        Collections.sort(columns, (ExpandedColumn c1, ExpandedColumn c2) -> 
                Integer.compare(c1.nodes().length(), c2.nodes().length())
        );
        Collections.reverse(columns);
        
        List<PrintWriter> writers = new ArrayList<>();
        for (int iFile = 0; iFile < fileCount; iFile++) {
            String filename = outputPrefix.replace("#", Integer.toString(iFile));
            File file = new File(filename);
            writers.add(FileSystem.openPrintWriter(file));
        }
        
        int index = 0;
        for (ExpandedColumn column : columns) {
            PrintWriter out = writers.get(index);
            if (groups.containsKey(column.id())) {
                for (int columnId : groups.get(column.id())) {
                    out.println(columnId);
                }
            } else {
                out.println(column.id());
            }
            index = (index + 1) % writers.size();
        }
        
        for (PrintWriter out : writers) {
            out.close();
        }
    }
    
    private static final String COMMAND =
            "Usage\n" +
            "  <eq-file>\n" +
            "  <file-count>\n" +
            "  <output-file-prefix>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnGroupPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        int fileCount = Integer.parseInt(args[1]);
        String outputFilePrefix = args[2];
        
        try {
            EQIndex nodeIndex = new EQIndex(eqFile);
            IdentifiableObjectSet<Column> db = nodeIndex.columns();
            new ColumnGroupPrinter().run(db, fileCount, outputFilePrefix);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
