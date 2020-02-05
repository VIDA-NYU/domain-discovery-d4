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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
import org.opendata.curation.d4.signature.SignatureBlocksGenerator;
import org.opendata.curation.d4.signature.SignatureBlocksIndex;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;
import org.opendata.curation.d4.signature.trim.SignatureTrimmerFactory;
import org.opendata.curation.d4.signature.trim.TrimmerType;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.util.MemUsagePrinter;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

/**
 * Expand columns using multiple threads. Each thread expands a single columns
 * while a given queue is not empty. This implementation relies on a in-memory
 * index of signature blocks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ParallelColumnExpander {
    
    public static final String TELEMETRY_ID = "EXPANDED COLUMNS";
    
    private class ExpanderTask implements Runnable {

        private final BigDecimal _decreaseFactor;
        private final ConcurrentLinkedQueue<ExpandedColumn> _columns;
        private final ExpandedColumnConsumer _consumer;
        private final int _id;
        private final EQIndex _nodes;
        private final int _numberOfIterations;
        private final SignatureBlocksIndex _signatures;
        private final Threshold _threshold;
        private final SignatureTrimmerFactory _trimmerFactory;

        public ExpanderTask(
                int id,
                EQIndex nodes,
                ConcurrentLinkedQueue<ExpandedColumn> columns,
                SignatureBlocksIndex signatures,
                SignatureTrimmerFactory trimmerFactory,
                Threshold threshold,
                BigDecimal decreaseFactor,
                int numberOfIterations,
                ExpandedColumnConsumer consumer
        ) {
            _id = id;
            _nodes = nodes;
            _columns = columns;
            _signatures = signatures;
            _trimmerFactory = trimmerFactory;
            _threshold = threshold;
            _decreaseFactor = decreaseFactor;
            _numberOfIterations = numberOfIterations;
            _consumer = consumer;
        }
        
        @Override
        public void run() {

            _consumer.open();
            
            int count = 0;
            
            Date start = new Date();
            
            ExpandedColumn column;
            while ((column = _columns.poll()) != null) {
                SingleColumnExpander expander;
                expander = new SingleColumnExpander(
                        _nodes,
                        column,
                        _threshold,
                        _decreaseFactor,
                        _numberOfIterations
                );
                while (!expander.isDone()) {
                    SignatureTrimmer trimmer;
                    trimmer = _trimmerFactory
                            .getTrimmer(column.nodes(), expander);
                    _signatures.stream(trimmer, expander.column().nodes());
                    column = expander.column();
                }
                _consumer.consume(column);
                count++;
            }
            
            Date end = new Date();
            
            _consumer.close();

            long execTime = end.getTime() - start.getTime();
            
            System.out.println(_id + " DONE WITH " + count + " COLUMNS IN " + execTime + " ms");
        }
    }
    
    private final TelemetryCollector _telemetry;
    
    public ParallelColumnExpander(TelemetryCollector telemetry) {
        
        _telemetry = telemetry;
    }
    
    public ParallelColumnExpander() {
        
        this(new TelemetryPrinter());
    }
    
    public void run(
            EQIndex nodes,
            SignatureBlocksIndex signatures,
            SignatureTrimmerFactory trimmerFactory,
            IdentifiableObjectSet<Column> db,
            IDSet columnFilter,
            Threshold threshold,
            BigDecimal decreaseFactor,
            int numberOfIterations,
            int threads,
            ExpandedColumnConsumerFactory consumerFactory
    ) {
        HashMap<String, ExpandedColumn> columnIndex = new HashMap<>();
        HashMap<Integer, HashIDSet> groups = new HashMap<>();
        HashMap<String, Integer> mapping = new HashMap<>();
        for (int columnId : columnFilter) {
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
        List<ExpandedColumn> columnList = new ArrayList<>(columnIndex.values());
        Collections.sort(columnList, (ExpandedColumn c1, ExpandedColumn c2) -> 
                Integer.compare(c1.nodes().length(), c2.nodes().length())
        );
        Collections.reverse(columnList);
        ConcurrentLinkedQueue<ExpandedColumn> columns;
        columns = new ConcurrentLinkedQueue<>(columnList);
        
        System.out.println(
                "EXPAND " + db.length() + " COLUMNS " +
                "IN " + columns.size() + " GROUPS " +
                "USING " + threads + " THREADS"
        );
        
        Date start = new Date();
        System.out.println("START @ " + start);
        
        new MemUsagePrinter().print();
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            ExpanderTask expander = new ExpanderTask(
                    iThread,
                    nodes,
                    columns,
                    signatures,
                    trimmerFactory,
                    threshold,
                    decreaseFactor,
                    numberOfIterations,
                    consumerFactory.getConsumer(groups)
            );
            es.execute(expander);
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        
        Date end = new Date();
        long execTime = end.getTime() - start.getTime();
        _telemetry.add(TELEMETRY_ID, execTime);
        
        System.out.println("END @ " + end);
        
        new MemUsagePrinter().print();
    }
    
    private static final String ARG_COLUMNS = "columns";
    private static final String ARG_DECREASE = "decrease";
    private static final String ARG_ITERATIONS = "iterations";
    private static final String ARG_OUTDIR = "outputToDir";
    private static final String ARG_THREADS = "threads";
    private static final String ARG_THRESHOLD = "threshold";
    private static final String ARG_TRIMMER = "trimmer";
    
    private static final String[] ARGS = {
        ARG_COLUMNS,
        ARG_DECREASE,
        ARG_ITERATIONS,
        ARG_OUTDIR,
        ARG_THREADS,
        ARG_THRESHOLD,
        ARG_TRIMMER
    };
    
    private static final String COMMAND =
            "Usage\n" +
            "  --" + ARG_DECREASE + "=<real> [default: 0.05]\n" +
            "  --" + ARG_ITERATIONS + "=<int> [default: 5]\n" +
            "  --" + ARG_COLUMNS + "=<column-list-file> [default: null]\n" +
            "  --" + ARG_OUTDIR + "=[true | false] [default: false]\n" +
            "  --" + ARG_THREADS + "=<int> [default: 6]\n" +
            "  --" + ARG_THRESHOLD + "=<constraint> [default: GT0.25]\n" +
            "  --" + ARG_TRIMMER + "=<signature-trimmer> [default: " +
                    TrimmerType.CENTRIST.toString() +"]\n" +
            "  <eq-file>\n" +
            "  <signature-file(s)>\n" +
            "  <output-file-or-directory>";
    
    private static final Logger LOGGER = Logger
            .getLogger(SignatureBlocksGenerator.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Column Expander - Version (" + Constants.VERSION + ")\n");

        if (args.length < 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Arguments params = new Arguments(ARGS, args, 3);
        File eqFile = new File(params.fixedArg(0));
        File signatureFile = new File(params.fixedArg(1));
        File output = new File(params.fixedArg(2));

        TrimmerType trimmer = TrimmerType
                .valueOf(params.getAsString(ARG_TRIMMER, TrimmerType.CENTRIST.toString()));
        Threshold threshold = Threshold.getConstraint(params.getAsString(ARG_THRESHOLD, "GT0.25"));
        int numberOfIterations = params.getAsInt(ARG_ITERATIONS, 5);
        BigDecimal decreaseFactor = params.getAsBigDecimal(ARG_DECREASE, new BigDecimal("0.05"));
        boolean outputToDir = params.getAsBool(ARG_OUTDIR, false);
        int threads = params.getAsInt(ARG_THREADS, 6);
                
        File columnsFile = null;
        if (params.has(ARG_COLUMNS)) {
            columnsFile = new File(params.get(ARG_COLUMNS));
        }
        
        try {
            // Read the node index and the list of columns
            EQIndex nodeIndex = new EQIndex(eqFile);
            IdentifiableObjectSet<Column> db = nodeIndex.columns();
            // Read the list of column identifier if a columns file was given
            IDSet columnFilter;
            if (columnsFile != null) {
                columnFilter = new HashIDSet(columnsFile);
            } else {
                columnFilter = db.keys();
            }
            SignatureBlocksIndex buffer = new SignatureBlocksIndex();
            new SignatureBlocksReader(signatureFile).stream(buffer);
            new ParallelColumnExpander().run(
                    nodeIndex,
                    buffer,
                    new SignatureTrimmerFactory(nodeIndex, trimmer),
                    db,
                    columnFilter,
                    threshold,
                    decreaseFactor,
                    numberOfIterations,
                    threads,
                    new ExpandedColumnWriterFactory(output, outputToDir)
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
