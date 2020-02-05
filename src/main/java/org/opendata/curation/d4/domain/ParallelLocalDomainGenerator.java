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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.Arguments;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksIndex;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;
import org.opendata.curation.d4.signature.trim.SignatureTrimmerFactory;
import org.opendata.curation.d4.signature.trim.TrimmerType;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.db.eq.EQIndex;

/**
 * Generator for local domains using undirected graphs. Each connected component
 * in the graph generated from the robust signatures of the column elements 
 * represents a local domain.
 * 
 * Uses a concurrent queue to distribute columns across workers. Relies
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ParallelLocalDomainGenerator {
                   
    public static final String TELEMETRY_ID = "LOCAL DOMAINS";

    private class DomainGeneratorTask implements Runnable {

        private final ConcurrentLinkedQueue<ExpandedColumn> _columns;
        private final UniqueDomainSet _domains;
        private final int _id;
        private final EQIndex _nodes;
        private final SignatureBlocksIndex _signatures;
        private final SignatureTrimmerFactory _trimmerFactory;
        
        public DomainGeneratorTask(
                int id,
                EQIndex nodes,
                ConcurrentLinkedQueue<ExpandedColumn> columns,
                SignatureBlocksIndex signatures,
                SignatureTrimmerFactory trimmerFactory,
                UniqueDomainSet domains
       ) {
            _id = id;
            _nodes = nodes;
            _columns = columns;
            _signatures = signatures;
            _trimmerFactory = trimmerFactory;
            _domains = domains;
        }
        
        @Override
        public void run() {

            int count = 0;
            
            Date start = new Date();
            
            ExpandedColumn column;
            while ((column = _columns.poll()) != null) {
                IDSet col = column.nodes();
                SignatureBlocksConsumer domainGenerator;
                domainGenerator = new UndirectedDomainGenerator(
                        column,
                        _domains,
                        _nodes.nodeSizes()
                );
                SignatureTrimmer trimmer;
                trimmer = _trimmerFactory.getTrimmer(col, domainGenerator);
                _signatures.stream(trimmer, col);
                count++;
            }
            
            Date end = new Date();
            
            long execTime = end.getTime() - start.getTime();
            
            System.out.println(_id + " DONE WITH " + count + " COLUMNS IN " + execTime + " ms");
        }
    }
    
    private final TelemetryCollector _telemetry;
    
    public ParallelLocalDomainGenerator(TelemetryCollector telemetry) {
        
        _telemetry = telemetry;
    }
    
    public ParallelLocalDomainGenerator() {
        
        this(new TelemetryPrinter());
    }
    
    public void run(
            EQIndex nodes,
            ExpandedColumnIndex columnIndex,
            SignatureBlocksIndex signatures,
            TrimmerType trimmer,
            int threads,
            DomainConsumer consumer
    ) throws java.io.IOException {

        UniqueDomainSet domains = new UniqueDomainSet(columnIndex);
        
        Date start = new Date();
        System.out.println("START @ " + start);

        ExecutorService es = Executors.newCachedThreadPool();
        
        // Sort column in decreasing number of nodes
        List<ExpandedColumn> columnList = new ArrayList<>(columnIndex.columns());
        Collections.sort(columnList, (ExpandedColumn c1, ExpandedColumn c2) -> 
                Integer.compare(c1.nodes().length(), c2.nodes().length())
        );
        Collections.reverse(columnList);
        ConcurrentLinkedQueue<ExpandedColumn> columns;
        columns = new ConcurrentLinkedQueue(columnList);
        
        for (int iThread = 0; iThread < threads; iThread++) {
            DomainGeneratorTask task = new DomainGeneratorTask(
                    iThread,
                    nodes,
                    columns,
                    signatures,
                    new SignatureTrimmerFactory(nodes, trimmer),
                    domains
            );
            es.execute(task);
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        
        domains.stream(consumer);
        
        Date end = new Date();
        System.out.println("END @ " + end);
        
        long execTime = end.getTime() - start.getTime();
        _telemetry.add(TELEMETRY_ID, execTime);
    }
    
    private static final String ARG_COLUMNS = "columns";
    private static final String ARG_THREADS = "threads";
    private static final String ARG_TRIMMER = "trimmer";
    
    private static final String[] ARGS = {
        ARG_COLUMNS,
        ARG_THREADS,
        ARG_TRIMMER
    };
    
    private static final String COMMAND =
            "Usage\n" +
            "  --" + ARG_COLUMNS + "=<column-list-file> [default: null]\n" +
            "  --" + ARG_THREADS + "=<int> [default: 6]\n" +
            "  --" + ARG_TRIMMER + "=<signature-trimmer> [default: " +  
                    TrimmerType.CENTRIST.toString() +"]\n" +
            "  <eq-file>\n" +
            "  <signature-file(s)>\n" +
            "  <columns-file>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ParallelLocalDomainGenerator.class.getName());
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Local Domain Generator (Undirected) - Version (" + Constants.VERSION + ")\n");

        if (args.length < 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Arguments params = new Arguments(ARGS, args, 4);
        File eqFile = new File(params.fixedArg(0));
        File signatureFile = new File(params.fixedArg(1));
        File columnFile = new File(params.fixedArg(2));
        File outputFile = new File(params.fixedArg(3));

        TrimmerType trimmer = TrimmerType.valueOf(
                params.getAsString(ARG_TRIMMER, TrimmerType.CENTRIST.toString())
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
            SignatureBlocksIndex buffer = new SignatureBlocksIndex();
            new SignatureBlocksReader(signatureFile).stream(buffer);
            new ParallelLocalDomainGenerator().run(
                    nodeIndex,
                    columnIndex,
                    buffer,
                    trimmer,
                    threads,
                    new DomainWriter(outputFile)
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
