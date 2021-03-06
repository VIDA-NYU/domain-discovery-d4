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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
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
import org.opendata.curation.d4.signature.RobustSignatureConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksDispatcher;
import org.opendata.curation.d4.signature.SignatureBlocksStream;

/**
 * Generator for local domains using undirected graphs. Each connected component
 * in the graph generated from the robust signatures of the column elements 
 * represents a local domain.
 * 
 * The single scan local domain generator scans through the set of signature
 * blocks exactly once (per thread) while generating the local domains. Requires
 * to have domain generators for all columns in memory (instead of having a
 * copy of all signature blocks in memory).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExternalMemLocalDomainGenerator {
                   
    public static final String TELEMETRY_ID = "LOCAL DOMAINS";

    private class DomainGeneratorTask implements Runnable {

        private final List<ExpandedColumn> _columns;
        private final UniqueDomainSet _domains;
        private final Integer[] _eqTermCounts;
        private final int _id;
        private final SignatureBlocksStream _signatures;
        private final SignatureTrimmerFactory _trimmerFactory;
        private final boolean _verbose;
        
        public DomainGeneratorTask(
                int id,
                Integer[] eqTermCounts,
                List<ExpandedColumn> columns,
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
            
            SignatureBlocksDispatcher dispatcher;
            dispatcher = new SignatureBlocksDispatcher();
            
            for (ExpandedColumn column : _columns) {
                RobustSignatureConsumer domainGenerator;
                domainGenerator = new UndirectedDomainGenerator(
                        column,
                        _domains,
                        _eqTermCounts
                );
                SignatureTrimmer trimmer;
                trimmer = _trimmerFactory.getSignatureTrimmer(column, domainGenerator);
                dispatcher.add(trimmer);
            }
            
            Date start = new Date();

            _signatures.stream(dispatcher);
            
            Date end = new Date();
            
            long execTime = end.getTime() - start.getTime();
            
            if (_verbose) {
                System.out.println(_id + " DONE WITH " + _columns.size() + " COLUMNS IN " + execTime + " ms");
            }
        }
    }
    
    private final TelemetryCollector _telemetry;
    
    public ExternalMemLocalDomainGenerator(TelemetryCollector telemetry) {
        
        _telemetry = telemetry;
    }
    
    public ExternalMemLocalDomainGenerator() {
        
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

        // Sort column in decreasing number of nodes
        List<ExpandedColumn> columnList = new ArrayList<>(columnIndex.asList());
        Collections.sort(columnList, (ExpandedColumn c1, ExpandedColumn c2) -> 
                Integer.compare(c1.nodes().length(), c2.nodes().length())
        );
        Collections.reverse(columnList);
        
        if (verbose) {
            System.out.println(
                    String.format(
                            "LOCAL DOMAINS (EXTERNAL MEMORY) FOR %d COLUMN GROUPS",
                            columnList.size()
                    )
            );
            System.out.println("START @ " + start);
        }
        
        ExecutorService es = Executors.newCachedThreadPool();
        
        for (int iThread = 0; iThread < threads; iThread++) {
            List<ExpandedColumn> columns = new ArrayList<>();
            for (int iCol = iThread; iCol < columnList.size(); iCol += threads) {
                columns.add(columnList.get(iCol));
            }
            DomainGeneratorTask task = new DomainGeneratorTask(
                    iThread,
                    eqTermCounts,
                    columns,
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
