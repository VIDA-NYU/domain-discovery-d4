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
package org.opendata.curation.d4.domain;

import org.opendata.curation.d4.domain.graph.GraphFileReader;
import java.io.File;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.graph.ConnectedComponentGenerator;
import org.opendata.core.graph.DirectedConnectedComponents;
import org.opendata.core.graph.UndirectedConnectedComponents;
import org.opendata.curation.d4.Arguments;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.db.eq.EQIndex;

/**
 * Generator for local domains using undirected graphs. Each connected component
 * in the graph generated from the robust signatures of the column elements 
 * represents a local domain.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LocalDomainGenerator {
                   
    public static final String TELEMETRY_ID = "LOCAL DOMAINS";

    private class DomainGeneratorTask implements Runnable {

        private final ConcurrentLinkedQueue<GraphFileReader> _columns;
        private final UniqueDomainSet _domains;
        private final EdgeType _edgeType;
        private final int[] _nodeSizes;
        private final int _thread;
        
        public DomainGeneratorTask(
                int thread,
                ConcurrentLinkedQueue<GraphFileReader> columns,
                EdgeType edgeType,
                int[] nodeSizes,
                UniqueDomainSet domains
        ) {
            _thread = thread;
            _columns = columns;
            _edgeType = edgeType;
            _nodeSizes = nodeSizes;
            _domains = domains;
        }
        
        @Override
        public void run() {

            System.out.println("THREAD " + _thread + " START @ " + new Date());
            
            GraphFileReader reader;
            while ((reader = _columns.poll()) != null) {
                System.out.println(_thread + "\t" + reader.file().getName() + " (" + _columns.size() + ")");
                IDSet colNodes = reader.column().nodes();
                ConnectedComponentGenerator domainGenerator;
                switch (_edgeType) {
                    case Directed:
                        domainGenerator = new DirectedConnectedComponents(colNodes);
                        break;
                    case Single:
                        domainGenerator = new UndirectedConnectedComponents(colNodes);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown edge type: " + _edgeType.toString());
                }
                DomainComponentGenerator compGen;
                compGen = new DomainComponentGenerator(
                        reader.column(),
                        domainGenerator,
                        _domains,
                        _nodeSizes
                );
                reader.run(compGen);
            }
            System.out.println(_thread + "\tDONE");
        }
    }
    
    private final TelemetryCollector _telemetry;
    
    public LocalDomainGenerator(TelemetryCollector telemetry) {
        
        _telemetry = telemetry;
    }
    
    public LocalDomainGenerator() {
        
        this(new TelemetryPrinter());
    }
    
    public void run(
            EQIndex nodes,
            ExpandedColumnIndex columnIndex,
            File graphDir,
            EdgeType edgeType,
            int threads,
            DomainConsumer consumer
    ) throws java.io.IOException {

        UniqueDomainSet domains = new UniqueDomainSet(columnIndex);
        
        Date start = new Date();
        System.out.println("START @ " + start);

        int[] nodeSizes = nodes.nodeSizes();
        
        List<ExpandedColumn> columns = columnIndex.columns();
        Collections.sort(columns, (ExpandedColumn c1, ExpandedColumn c2) -> {
            return Integer.compare(c2.length(), c1.length());
        });
        
        ConcurrentLinkedQueue<GraphFileReader> workers;
        workers = new ConcurrentLinkedQueue<>();
        for (ExpandedColumn column : columns) {
            String filename = column.id() + ".txt.gz";
            File graphFile = FileSystem.joinPath(graphDir, filename);
            workers.add(new GraphFileReader(graphFile, column));
        }

        System.out.println("USING " + threads + " THREADS FOR " + workers.size() + " WORKERS @ " + new Date());
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(new DomainGeneratorTask(iThread, workers, edgeType, nodeSizes, domains));
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        System.out.println("WRITE DOMAINS");
        
        domains.stream(consumer);
        
        Date end = new Date();
        System.out.println("END @ " + end);
        
        long execTime = end.getTime() - start.getTime();
        _telemetry.add(TELEMETRY_ID, execTime);
    }
    
    private static final String ARG_COLUMNS = "columns";
    private static final String ARG_EDGETYPE = "edges";
    private static final String ARG_THREADS = "threads";
    
    private static final String[] ARGS = {
        ARG_COLUMNS,
        ARG_EDGETYPE,
        ARG_THREADS
    };
    
    private static final String COMMAND =
            "Usage\n" +
            "  --" + ARG_COLUMNS + "=<column-list-file> [default: null]\n" +
            "  --" + ARG_EDGETYPE + "=<edge-type> [default: " + EdgeType.Single + "]\n" +
            "  --" + ARG_THREADS + "=<int> [default: 6]\n" +
            "  <eq-file>\n" +
            "  <columns-file>\n" +
            "  <graph-dir>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(LocalDomainGenerator.class.getName());
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Local Domain Generator - Version (" + Constants.VERSION + ")\n");

        if (args.length < 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Arguments params = new Arguments(ARGS, args, 4);
        File eqFile = new File(params.fixedArg(0));
        File columnFile = new File(params.fixedArg(1));
        File graphDir = new File(params.fixedArg(2));
        File outputFile = new File(params.fixedArg(3));

        EdgeType edgeType = EdgeType.valueOf(
                params.getAsString(ARG_EDGETYPE, EdgeType.Single.toString())
        );
        int threads = params.getAsInt(ARG_THREADS, 6);
                
        File columnsFile = null;
        if (params.has(ARG_COLUMNS)) {
            columnsFile = new File(params.get(ARG_COLUMNS));
        }
        
        FileSystem.createParentFolder(outputFile);
        
        try {
            // Read the node index and the list of columns
            EQIndex nodeIndex = new EQIndex(eqFile);
            // Read the list of column identifier if a columns file was given
            ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
            if (columnsFile != null) {
                 new ExpandedColumnReader(columnFile)
                         .stream(columnIndex, new HashIDSet(columnsFile));
            } else {
                 new ExpandedColumnReader(columnFile).stream(columnIndex);
            }
            new LocalDomainGenerator().run(
                    nodeIndex,
                    columnIndex,
                    graphDir,
                    edgeType,
                    threads,
                    new DomainWriter(outputFile)
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
