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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;
import org.opendata.curation.d4.signature.trim.SignatureTrimmerFactory;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.util.MemUsagePrinter;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;
import org.opendata.curation.d4.signature.RobustSignatureDispatcher;
import org.opendata.curation.d4.signature.sketch.SignatureBlocksSketchFactory;
import org.opendata.db.column.Column;
import org.opendata.curation.d4.signature.RobustSignatureStream;
import org.opendata.db.Database;
import org.opendata.db.eq.EQReader;

/**
 * Expand columns using multiple threads. Each thread expands a single columns
 * while a given queue is not empty. This implementation relies on a in-memory
 * index of signature blocks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ParallelColumnExpander {
    
    public static final String TELEMETRY_ID = "EXPANDED COLUMNS";
    
    private final static Logger LOGGER = Logger
            .getLogger(ParallelColumnExpander.class.getName());
    
    private class ExpanderTask implements Runnable {

        private final BigDecimal _decreaseFactor;
        private final List<ExpandedColumn> _columns;
        private final ExpandedColumnConsumer _consumer;
        private final Integer[] _eqTermCounts;
        private final int _id;
        private final int _numberOfIterations;
        private final RobustSignatureStream _signatures;
        private final SignatureBlocksSketchFactory _sketchFactory;
        private final Threshold _threshold;
        private final SignatureTrimmerFactory _trimmerFactory;
        private final boolean _verbose;
        
        public ExpanderTask(
                int id,
                Integer[] eqTermCounts,
                List<ExpandedColumn> columns,
                RobustSignatureStream signatures,
                SignatureTrimmerFactory trimmerFactory,
                SignatureBlocksSketchFactory sketchFactory,
                Threshold threshold,
                BigDecimal decreaseFactor,
                int numberOfIterations,
                boolean verbose,
                ExpandedColumnConsumer consumer
        ) {
            _id = id;
            _eqTermCounts = eqTermCounts;
            _columns = columns;
            _signatures = signatures;
            _trimmerFactory = trimmerFactory;
            _sketchFactory = sketchFactory;
            _threshold = threshold;
            _decreaseFactor = decreaseFactor;
            _numberOfIterations = numberOfIterations;
            _verbose = verbose;
            _consumer = consumer;
        }
        
        @Override
        public void run() {

            _consumer.open();
            
            Date start = new Date();
            
            List<SingleColumnExpander> columns;
            columns = new ArrayList<>();
            for (ExpandedColumn column : _columns) {
                    SingleColumnExpander expander;
                    expander = new SingleColumnExpander(
                            _eqTermCounts,
                            column,
                            _threshold,
                            _decreaseFactor,
                            _numberOfIterations
                    );
                    columns.add(expander);
            }
            int round = 0;
            while (!columns.isEmpty()) {
                RobustSignatureDispatcher dispatcher;
                dispatcher = new RobustSignatureDispatcher();
                for (SingleColumnExpander expander : columns) {
                    SignatureTrimmer trimmer;
                    trimmer = _trimmerFactory
                            .getTrimmer(
                                    expander.column().id(),
                                    expander
                            );
                    dispatcher.add(trimmer);
                }
                round++;
                if (_verbose) {
                    LOGGER.log(
                            Level.INFO,
                            String.format(
                                    "%d ROUND %d WITH %d columns",
                                    _id,
                                    round,
                                    columns.size()
                            )
                    );
                }
                RobustSignatureConsumer consumer;
                consumer = _sketchFactory.getConsumer(dispatcher);
                try {
                    _signatures.stream(consumer);
                } catch (java.io.IOException ex) {
                    throw new RuntimeException(ex);
                }
                List<SingleColumnExpander> candidates = new ArrayList<>();
                for (SingleColumnExpander expander : columns) {
                    if (expander.isDone()) {
                        _consumer.consume(expander.column());
                    } else {
                        candidates.add(expander);
                    }
                }
                columns = candidates;
            }
            
            Date end = new Date();
            
            _consumer.close();

            long execTime = end.getTime() - start.getTime();
            
            if (_verbose) {
                LOGGER.log(
                        Level.INFO,
                        String.format(
                                "%d DONE WITH %d COLUMNS IN %d ms",
                                _id,
                                _columns.size(),
                                execTime
                        )
                );
            }
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
            EQReader eqReader,
            RobustSignatureStream signatures,
            String trimmer,
            SignatureBlocksSketchFactory sketchFactory,
            Threshold threshold,
            BigDecimal decreaseFactor,
            int numberOfIterations,
            int threads,
            boolean verbose,
            File outputFile
    ) {
        HashMap<String, ExpandedColumn> columnIndex = new HashMap<>();
        HashMap<Integer, HashIDSet> groups = new HashMap<>();
        HashMap<String, Integer> mapping = new HashMap<>();
        
        IdentifiableObjectSet<Column> columns = new Database(eqReader).columns();
        for (Column column : columns) {
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
        
        Date start = new Date();
        if (verbose) {
            System.out.println(
                    String.format(
                            "EXPAND %d COLUMNS IN %d GROUPS USING:\n" +
                            "  --eqs=%s\n" +
                            "  --signatures=%s\n" +
                            "  --trimmer=%s\n" +
                            "  --sketch=%s\n" +
                            "  --expandThreshold=%s\n" +
                            "  --decrease=%s\n" +
                            "  --iterations=%d\n" +
                            "  --threads=%d\n" +
                            "  --columns=%s",
                            columns.length(),
                            columnList.size(),
                            eqReader.source(),
                            signatures.source(),
                            trimmer,
                            sketchFactory.toDocString(),
                            threshold.toPlainString(),
                            decreaseFactor.toPlainString(),
                            numberOfIterations,
                            threads,
                            outputFile.getName()
                    )
            );
            LOGGER.log(Level.INFO, String.format("START @ %s", start));
            new MemUsagePrinter().print();
        }
                
        SignatureTrimmerFactory trimmerFactory;
        trimmerFactory = new SignatureTrimmerFactory(eqReader, columns, trimmer);
        
        ExpandedColumnWriter writer;
        writer = new ExpandedColumnWriter(outputFile, groups);
        
        writer.open();
        
        Integer[] eqTermCounts = eqReader.readTermCounts();
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            List<ExpandedColumn> taskColumns = new ArrayList<>();
            for (int iCol = iThread; iCol < columnList.size(); iCol += threads) {
                taskColumns.add(columnList.get(iCol));
            }
            ExpanderTask expander = new ExpanderTask(
                    iThread,
                    eqTermCounts,
                    taskColumns,
                    signatures,
                    trimmerFactory,
                    sketchFactory,
                    threshold,
                    decreaseFactor,
                    numberOfIterations,
                    verbose,
                    writer
            );
            es.execute(expander);
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        
        writer.close();
        
        Date end = new Date();
        
        if (verbose) {
            long execTime = end.getTime() - start.getTime();
            _telemetry.add(TELEMETRY_ID, execTime);
            LOGGER.log(Level.INFO, String.format("END @ %s", end));
            new MemUsagePrinter().print();
        }
    }
}
