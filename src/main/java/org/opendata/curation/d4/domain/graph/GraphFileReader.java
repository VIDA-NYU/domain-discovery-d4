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
package org.opendata.curation.d4.domain.graph;

import java.io.BufferedReader;
import java.io.File;
import java.util.HashMap;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.IDSet;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.domain.EdgeConsumer;

/**
 * Read a graph file. The file contains one line per node with adjacent edges.
 * The list of edges are a binary array encoded as a hexadecimal string.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class GraphFileReader {
    
    private final ExpandedColumn _column;
    private final File _file;
    private final ColumnEdgeReader _parser;
    private final boolean _verbose;
            
    public GraphFileReader(File file, ExpandedColumn column, boolean verbose) {
        
        _file = file;
        _column = column;
        _parser = new HexEdgeReader(column);
        _verbose = verbose;
    }

    public ExpandedColumn column() {
        
        return _column;
    }
    
    public File file() {
        
        return _file;
    }
    
    public void run(EdgeConsumer consumer) {

        IDSet nodes =_column.nodes();
        HashMap<Integer, Integer> mapping = new HashMap<>();
        for (int nodeId : nodes.toArray()) {
            mapping.put(mapping.size(), nodeId);
        }
        int lineCount = 0;
        
        if (_file.exists()) {
            try (BufferedReader in = FileSystem.openReader(_file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    int nodeId = Integer.parseInt(tokens[0]);
                    consumer.consume(nodeId, _parser.parseLine(tokens[1]));
                    lineCount++;
                    if (((lineCount % 10000) == 0) && (_verbose)) {
                        System.out.println("READ " + lineCount + " LINES");
                    }
                }
            } catch (java.io.IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        
        if (_verbose) {
            System.out.println("READ " + lineCount + " LINES");
        }
        
        consumer.close();
    }
}
