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
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.constraint.ZeroThreshold;
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
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.HashIDSet;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksGenerator;
import org.opendata.curation.d4.signature.trim.CentristTrimmer;
import org.opendata.curation.d4.signature.trim.ConservativeTrimmer;
import org.opendata.curation.d4.signature.trim.LiberalTrimmer;
import org.opendata.curation.d4.signature.trim.NonTrimmer;
import org.opendata.curation.d4.signature.trim.PrecisionScore;
import org.opendata.curation.d4.signature.trim.TrimmerType;
import org.opendata.db.eq.EQIndex;

/**
 * Generator for local domains.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LocalDomainGenerator {
                   
    public static final String TELEMETRY_ID = "LOCAL DOMAINS";

    private class DomainGenerator implements SignatureBlocksConsumer {

        private final ExpandedColumn _column;
        private final ConnectedComponentGenerator _compGen;

        public DomainGenerator(
                ExpandedColumn column,
                ConnectedComponentGenerator compGen
        ) {
            _column = column;
            _compGen = compGen;
        }
        
        @Override
        public void close() {

        }

        @Override
        public synchronized void consume(SignatureBlocks sig) {

            final int sigId = sig.id();
            
            if (_column.contains(sigId)) {
                HashIDSet edges = new HashIDSet();
                for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
                    for (int nodeId : sig.get(iBlock)) {
                        if (_column.contains(nodeId)) {
                            edges.add(nodeId);
                        }
                    }
                }
                _compGen.add(sigId, edges.toArray());
            }
        }

        @Override
        public void open() {

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
            EQIndex eqIndex,
            ExpandedColumnIndex columnIndex,
            String trimmer,
            EdgeType edgeType,
            int threads,
            DomainConsumer writer
    ) throws java.lang.InterruptedException, java.io.IOException {

        UniqueDomainSet domains = new UniqueDomainSet(columnIndex);
        
        Date start = new Date();
        System.out.println("START @ " + start);

        
        List<ExpandedColumn> columns = columnIndex.columns();
        Collections.sort(columns, (ExpandedColumn c1, ExpandedColumn c2) -> {
            return Integer.compare(c2.length(), c1.length());
        });
        
        int[] nodeSizes = eqIndex.nodeSizes();
        
        TrimmerType trimmerType;
        Threshold threshold;
        
        if (trimmer.contains(":")) {
            String[] tokens = trimmer.split(":");
            trimmerType = TrimmerType.valueOf(tokens[0]);
            threshold = Threshold.getConstraint(tokens[1]);
        } else {
            trimmerType = TrimmerType.valueOf(trimmer);
            threshold = new GreaterThanConstraint(BigDecimal.ZERO);
        }
        
        for (ExpandedColumn column : columns) {
            System.out.println(column.id() + " (" + column.length() + ")");
            ConnectedComponentGenerator domGen;
            switch (edgeType) {
                case Directed:
                    domGen = new DirectedConnectedComponents(column.nodes());
                    break;
                case Single:
                    domGen = new UndirectedConnectedComponents(column.nodes());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown edge type: " + edgeType.toString());
            }
            SignatureBlocksConsumer consumer;
            consumer = new DomainGenerator(column, domGen);
            switch (trimmerType) {
                case CONSERVATIVE:
                    consumer = new ConservativeTrimmer(column.nodes(), consumer);
                    break;
                case CENTRIST:
                    consumer = new CentristTrimmer(
                            column.nodes(),
                            nodeSizes,
                            new PrecisionScore(),
                            new MaxDropFinder<>(threshold, false, false),
                            new ZeroThreshold(),
                            consumer
                    );
                    consumer = new LiberalTrimmer(eqIndex.nodeSizes(), consumer);
                    break;
                default:
                    consumer = new NonTrimmer(column.nodes(), consumer);
            }
            ConcurrentLinkedQueue<Integer> queue;
            queue = new ConcurrentLinkedQueue<>(column.nodes().toList());
            new SignatureBlocksGenerator()
                    .runWithMaxDrop(eqIndex, queue, false, true, threads, consumer);
        }

        System.out.println("WRITE DOMAINS");
        
        domains.stream(writer);
        
        Date end = new Date();
        System.out.println("END @ " + end);
        
        long execTime = end.getTime() - start.getTime();
        _telemetry.add(TELEMETRY_ID, execTime);
    }
    
    private static final String ARG_COLUMNS = "columns";
    private static final String ARG_EDGETYPE = "edges";
    private static final String ARG_THREADS = "threads";
    private static final String ARG_TRIMMER = "trimmer";
    
    private static final String[] ARGS = {
        ARG_COLUMNS,
        ARG_EDGETYPE,
        ARG_THREADS,
        ARG_TRIMMER
    };
    
    private static final String COMMAND =
            "Usage\n" +
            "  --" + ARG_COLUMNS + "=<column-list-file> [default: null]\n" +
            "  --" + ARG_EDGETYPE + "=<edge-type> [default: " + EdgeType.Single + "]\n" +
            "  --" + ARG_THREADS + "=<int> [default: 6]\n" +
            "  --" + ARG_TRIMMER + "=<signature-trimmer> [default: " +  
                    TrimmerType.CENTRIST.toString() +":GT0.1]\n" +
            "  <eq-file>\n" +
            "  <columns-file>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(LocalDomainGenerator.class.getName());
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Local Domain Generator - Version (" + Constants.VERSION + ")\n");

        if (args.length < 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Arguments params = new Arguments(ARGS, args, 3);
        File eqFile = new File(params.fixedArg(0));
        File columnFile = new File(params.fixedArg(1));
        File outputFile = new File(params.fixedArg(2));

        String trimmer = params
                .getAsString(ARG_TRIMMER, TrimmerType.CENTRIST.toString() + ":GT0.1");
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
                    trimmer,
                    edgeType,
                    threads,
                    new DomainWriter(outputFile)
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
