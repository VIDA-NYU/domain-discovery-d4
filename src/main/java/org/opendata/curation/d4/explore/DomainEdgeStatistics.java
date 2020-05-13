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
package org.opendata.curation.d4.explore;

import org.opendata.curation.d4.domain.graph.GraphFileReader;
import java.io.File;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.curation.d4.domain.EdgeConsumer;

/**
 * Print total number of edges for a given directory of column edge files.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainEdgeStatistics {
    
    private class EdgeCounter implements EdgeConsumer {

        private long _edgeCount = 0;
        private int _nodeCount = 0;
        private final HashIDSet _nodes = new HashIDSet();
        
        @Override
        public void close() {

        }

        @Override
        public void consume(int nodeId, List<Integer> edges) {

            _nodeCount++;
            _edgeCount += edges.size();
            
            if (!_nodes.contains(nodeId)) {
                _nodes.add(nodeId);
            }
        }
        
        public long edgeCount() {
            
            return _edgeCount;
        }
        
        public int nodeCount() {
            
            return _nodeCount;
        }
        
        public HashIDSet nodes() {
            
            return _nodes;
        }
    }
    
    public void run(
            ExpandedColumnIndex columnIndex,
            File graphDir
    ) throws java.io.IOException {

        Date start = new Date();
        System.out.println("START @ " + start);

        List<ExpandedColumn> columns = columnIndex.columns();
        Collections.sort(columns, (ExpandedColumn c1, ExpandedColumn c2) -> {
            return Integer.compare(c2.length(), c1.length());
        });
        
        HashIDSet nodes = new HashIDSet();
        EdgeCounter counter = new EdgeCounter();
        for (ExpandedColumn column : columns) {
            String filename = column.id() + ".txt.gz";
            File graphFile = FileSystem.joinPath(graphDir, filename);
            if (graphFile.exists()) {
                nodes.add(column.nodes());
                System.out.print(filename);
                new GraphFileReader(graphFile, column, false).run(counter);
                System.out.println("\t" + counter.nodeCount() + "\t" + counter.edgeCount());
            }
        }
        
        IDSet nodesWithoutEdges = nodes.difference(counter.nodes());
        
        System.out.println("TOTAL COUNTS: " + counter.nodeCount() + " NODES; " + counter.edgeCount() + " EDGES.");
        System.out.println("NODES WITH EDGES   : " + counter.nodes().length());
        System.out.println("NODES WITHOUT EDGES: " + nodesWithoutEdges.length());
        System.out.println();
        for (int nodeId : nodesWithoutEdges) {
            System.out.println(nodeId);
        }
    }

    private static final String COMMAND =
            "Usage\n" +
            "  <columns-file>\n" +
            "  <graph-dir>";
    
    private static final Logger LOGGER = Logger
            .getLogger(DomainEdgeStatistics.class.getName());
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Edge Stats Printer - Version (" + Constants.VERSION + ")\n");

        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File columnFile = new File(args[0]);
        File graphDir = new File(args[1]);
        
        try {
            // Read the list of column identifier if a columns file was given
            ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
            new ExpandedColumnReader(columnFile).stream(columnIndex);
            new DomainEdgeStatistics().run(columnIndex, graphDir);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
