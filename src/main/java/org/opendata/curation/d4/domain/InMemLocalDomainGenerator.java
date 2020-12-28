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

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;
import org.opendata.curation.d4.SignatureTrimmerFactory;
import org.opendata.core.set.MutableIdentifiableIDSet;
import org.opendata.core.util.MemUsagePrinter;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksStream;

/**
 * Generator for local domains using undirected graphs. Each connected component
 * in the graph generated from the robust signatures of the column elements 
 * represents a local domain.
 * 
 * The multi scan local domain generator reads the set of signature blocks into
 * main memory and scans through the set for each column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class InMemLocalDomainGenerator {
                   
    public static final String TELEMETRY_ID = "LOCAL DOMAINS";

    private class DomainGeneratorTask implements Runnable {

        private final ConcurrentLinkedQueue<ExpandedColumn> _columns;
        private final UniqueDomainSet _domains;
        private final Integer[] _eqTermCounts;
        private final int _id;
        private final SignatureBlocksStream _signatures;
        private final SignatureTrimmerFactory _trimmerFactory;
        private final boolean _verbose;
        
        public DomainGeneratorTask(
                int id,
                Integer[] eqTermCounts,
                ConcurrentLinkedQueue<ExpandedColumn> columns,
                SignatureBlocksStream signatures,
                SignatureTrimmerFactory trimmerFactory,
                UniqueDomainSet domains,
                boolean verbose
       ) {
            _id = id;
            _eqTermCounts = eqTermCounts;
            _columns = columns;
            _signatures = signatures;
            _trimmerFactory = trimmerFactory;
            _domains = domains;
            _verbose = verbose;
        }
        
        @Override
        public void run() {
            
            Date start = new Date();

            ExpandedColumn column;
            while ((column = _columns.poll()) != null) {
                RobustSignatureConsumer domainGenerator;
                domainGenerator = new UndirectedDomainGenerator(
                        column,
                        _domains,
                        _eqTermCounts
                );
                SignatureTrimmer trimmer;
                trimmer = _trimmerFactory.getSignatureTrimmer(column, domainGenerator);
                Date runStart = new Date();
                _signatures.stream(trimmer);
                Date runEnd = new Date();
                System.out.println(column.id() + " (" + column.totalSize() + "): " + (runEnd.getTime() - runStart.getTime()) + " ms");
            }
            
            Date end = new Date();
            
            long execTime = end.getTime() - start.getTime();
            
            if (_verbose) {
                System.out.println(_id + " DONE WITH " + _columns.size() + " COLUMNS IN " + execTime + " ms");
            }
        }
    }
    
    private final TelemetryCollector _telemetry;
    
    public InMemLocalDomainGenerator(TelemetryCollector telemetry) {
        
        _telemetry = telemetry;
    }
    
    public InMemLocalDomainGenerator() {
        
        this(new TelemetryPrinter());
    }
    
    public void run(
            Integer[] eqTermCounts,
            ExpandedColumnIndex columnIndex,
            SignatureBlocksStream signatures,
            SignatureTrimmerFactory trimmerFactory,
            int threads,
            boolean verbose,
            DomainConsumer consumer
    ) throws java.io.IOException {

        UniqueDomainSet domains = new UniqueDomainSet(columnIndex);
        
        Date start = new Date();
        
        ExecutorService es = Executors.newCachedThreadPool();
        
        List<ExpandedColumn> columnList = columnIndex.asList();
        Collections.sort(columnList, new Comparator<ExpandedColumn>(){
            @Override
            public int compare(ExpandedColumn col1, ExpandedColumn col2) {
                return Integer.compare(col2.totalSize(), col1.totalSize());
            }
        });
        ConcurrentLinkedQueue<ExpandedColumn> queue;
        queue = new ConcurrentLinkedQueue<>(columnList);
        
        if (verbose) {
            System.out.println(
                    String.format(
                            "LOCAL DOMAINS (IN MEMORY) FOR %d COLUMN GROUPS",
                            columnList.size()
                    )
            );
            System.out.println(String.format("START @ %s", start));
            new MemUsagePrinter().print();
        }
        
        for (int iThread = 0; iThread < threads; iThread++) {
            DomainGeneratorTask task = new DomainGeneratorTask(
                    iThread,
                    eqTermCounts,
                    queue,
                    signatures,
                    trimmerFactory,
                    domains,
                    verbose
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
        if (verbose) {
            System.out.println("END @ " + end);
            long execTime = end.getTime() - start.getTime();
            _telemetry.add(TELEMETRY_ID, execTime);
        }
    }
}
