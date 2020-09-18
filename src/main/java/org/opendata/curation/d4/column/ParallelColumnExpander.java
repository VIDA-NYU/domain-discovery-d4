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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.curation.d4.signature.SignatureBlocksStream;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;
import org.opendata.curation.d4.signature.trim.SignatureTrimmerFactory;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.util.MemUsagePrinter;
import org.opendata.curation.d4.signature.SignatureBlocksDispatcher;
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
        private final List<ExpandedColumn> _columns;
        private final ExpandedColumnConsumer _consumer;
        private final int _id;
        private final EQIndex _nodes;
        private final int _numberOfIterations;
        private final SignatureBlocksStream _signatures;
        private final Threshold _threshold;
        private final SignatureTrimmerFactory _trimmerFactory;

        public ExpanderTask(
                int id,
                EQIndex nodes,
                List<ExpandedColumn> columns,
                SignatureBlocksStream signatures,
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
            
            Date start = new Date();
            
            List<SingleColumnExpander> columns;
            columns = new ArrayList<>();
            for (ExpandedColumn column : _columns) {
                    SingleColumnExpander expander;
                    expander = new SingleColumnExpander(
                            _nodes,
                            column,
                            _threshold,
                            _decreaseFactor,
                            _numberOfIterations
                    );
                    columns.add(expander);
            }
            while (!columns.isEmpty()) {
                SignatureBlocksDispatcher dispatcher;
                dispatcher = new SignatureBlocksDispatcher();
                for (SingleColumnExpander expander : columns) {
                    SignatureTrimmer trimmer;
                    trimmer = _trimmerFactory
                            .getTrimmer(expander.column().nodes(), expander);
                    dispatcher.add(trimmer);
                }
                try {
                    _signatures.stream(dispatcher);
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
            
            System.out.println(_id + " DONE WITH " + _columns.size() + " COLUMNS IN " + execTime + " ms");
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
            SignatureBlocksStream signatures,
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
        
        System.out.println(
                "EXPAND " + db.length() + " COLUMNS " +
                "IN " + columnList.size() + " GROUPS " +
                "USING " + threads + " THREADS"
        );
        
        Date start = new Date();
        System.out.println("START @ " + start);
        
        new MemUsagePrinter().print();
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            List<ExpandedColumn> columns = new ArrayList<>();
            for (int iCol = iThread; iCol < columnList.size(); iCol += threads) {
                columns.add(columnList.get(iCol));
            }
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
}
