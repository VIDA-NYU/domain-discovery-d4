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
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.graph.UndirectedConnectedComponents;
import org.opendata.core.object.IdentifiableObjectImpl;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.util.count.IdentifiableCounterSet;
import org.opendata.db.eq.EQIndex;

/**
 * Identify domains that have support by at least n other domains. Support
 * between domains is defined as overlap above a given threshold.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class StrongDomainGenerator {
    
    public static final String TELEMETRY_ID = "STRONG DOMAINS";

    /**
     * Estimate frequency for the semantic type of a local domain. Frequency is
     * estimated based on the number of columns that contain overlapping local
     * domains for a given threshold.
     */
    private class DomainFrequencyEstimator implements Runnable {

        private final IdentifiableObjectSet<Domain> _domains;
        private final DomainHelper _helper;
        private final ConcurrentLinkedQueue<Domain> _queue;
        private final Threshold _supportConstraint;
        private final HashMap<Integer, HashIDSet> _typeColumns;

        public DomainFrequencyEstimator(
                ConcurrentLinkedQueue<Domain> queue,
                IdentifiableObjectSet<Domain> domains,
                Threshold supportConstraint,
                DomainHelper helper,
                HashMap<Integer, HashIDSet> typeColumns
        ) {
            _queue = queue;
            _domains = domains;
            _supportConstraint = supportConstraint;
            _helper = helper;
            _typeColumns = typeColumns;
        }

        @Override
        public void run() {

            Domain domI;
            while ((domI = _queue.poll()) != null) {
                for (Domain domJ : _domains) {
                    if (domI.id() < domJ.id()) {
                        BigDecimal ovp = _helper.termOverlap(domI, domJ);
                        if (_supportConstraint.isSatisfied(ovp)) {
                            HashIDSet colsI =_typeColumns.get(domI.id());
                            synchronized(this) {
                                colsI.add(domJ.columns());
                            }
                            HashIDSet colsJ = _typeColumns.get(domJ.id());
                            synchronized(this) {
                                colsJ.add(domI.columns());
                            }
                        }
                    }
                }
            }

        }
    }
    
    /**
     * Maintain list of supporting local domains.
     */
    private class DomainSupport extends IdentifiableObjectImpl {
        
        private final IDSet _supportSet;
        
        public DomainSupport(int id, IDSet supportSet) {
            
            super(id);
            
            _supportSet = supportSet;
        }
        
        /**
         * Get the set of identifier for all local domains that supported this
         * domain.
         * 
         * @return 
         */
        public IDSet supportSet() {
            
            return _supportSet;
        }
    }
    
    /**
     * Compute support for a local domain. Support for a local domain is based
     * on overlap with domains in columns that where estimated to contain the
     * same semantic type.
     */
    private class DomainSupportComputer implements Runnable {

        private final IdentifiableObjectSet<Domain> _domains;
        private final IdentifiableCounterSet _frequencyEstimate;
        private final DomainHelper _helper;
        private final Threshold _overlapConstraint;
        private final ConcurrentLinkedQueue<Domain> _queue;
        private final HashObjectSet<DomainSupport> _strongDomains;
        private final BigDecimal _supportFraction;
        private final boolean _verbose;
        
        public DomainSupportComputer(
                ConcurrentLinkedQueue<Domain> queue,
                IdentifiableObjectSet<Domain> domains,
                Threshold overlapConstraint,
                BigDecimal supportFraction,
                IdentifiableCounterSet frequencyEstimate,
                DomainHelper helper,
                boolean verbose,
                HashObjectSet<DomainSupport> strongDomains
        ) {
            _queue = queue;
            _domains = domains;
            _overlapConstraint = overlapConstraint;
            _supportFraction = supportFraction;
            _frequencyEstimate = frequencyEstimate;
            _helper = helper;
            _verbose = verbose;
            _strongDomains = strongDomains;
        }
        
        @Override
        public void run() {

            Domain domain;
            while ((domain = _queue.poll()) != null) {
                HashIDSet columns = new HashIDSet(domain.columns());
                HashIDSet support = new HashIDSet();
                for (Domain domI : _domains) {
                    if (domain.id() != domI.id()) {
                        if (domain.overlaps(domI)) {
                            BigDecimal ji = _helper.termOverlap(domain, domI);
                            if (_overlapConstraint.isSatisfied(ji)) {
                                columns.add(domI.columns());
                                support.add(domI.id());
                            }
                        }
                    }
                }
                // Minimum number of columns the domain needs support from to
                // be considered a strong domain.
                int frequency = _frequencyEstimate.get(domain.id()).value();
                int minColumnCount = ((int)Math.floor(
                        new BigDecimal(frequency)
                            .multiply(_supportFraction)
                            .doubleValue()
                )) + 1;
                int edgeCount = columns.length() - 1;
                // If the support constraint is satisfied add the domain to the
                // set of strong domains. Add edges in the support graph with
                // all local domains that provided support.
                if (edgeCount >= minColumnCount) {
                    synchronized(this) {
                        _strongDomains.add(
                                new DomainSupport(domain.id(), support)
                        );
                    }
                }
                if (_verbose) {
                    System.out.println(
                            domain.id() + "\t" +
                            domain.columns().length() + "\t" +
                            frequency + "\t" +
                            minColumnCount + "\t" +
                            edgeCount
                    );
                }
            }
        }
    }

    private final TelemetryCollector _telemetry;
    
    public StrongDomainGenerator(TelemetryCollector telemetry) {
        
        _telemetry = telemetry;
    }
    
    public StrongDomainGenerator() {
        
        this(new TelemetryPrinter());
    }

    /**
     * Compute set local domains that have sufficient support.
     * 
     * @param nodes
     * @param domainReader
     * @param domainOverlapConstraint
     * @param minSupportConstraint
     * @param supportFraction
     * @param verbose
     * @param threads
     * @param outputFile
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException 
     */
    public void run(
            EQIndex nodes,
            DomainReader domainReader,
            Threshold domainOverlapConstraint,
            Threshold minSupportConstraint,
            BigDecimal supportFraction,
            boolean verbose,
            int threads,
            File outputFile
    ) throws java.lang.InterruptedException, java.io.IOException {

        IdentifiableObjectSet<Domain> localDomains;
        localDomains = domainReader.read();
        
        Date start = new Date();
        if (verbose) {
            System.out.println(
                    String.format(
                            "STRONG DOMAINS FOR %d LOCAL DOMAINS USING:\n" +
                            "  --eqs=%s\n" +                            
                            "  --localdomains=%s\n" +
                            "  --domainOverlap=%s\n" +
                            "  --minSupport=%s\n" +
                            "  --supportFraction=%s\n" +
                            "  --threads=%d\n" +
                            "  --strongdomains=%s",
                            localDomains.length(),
                            nodes.source(),
                            domainReader.source(),
                            domainOverlapConstraint.toPlainString(),
                            minSupportConstraint.toPlainString(),
                            supportFraction.toPlainString(),
                            threads,
                            outputFile.getName()
                    )
            );
        }
        
        DomainHelper helper = new DomainHelper(nodes, localDomains);
        
        // For each local domain we first estimate the frequency of the semantic
        // type that the domain represents in the database. The estimate is
        // based on the number of columns that contain a local domain that
        // overlaps with the given domain at a low (minSupportConstraint)
        // threshold.
        HashMap<Integer, HashIDSet> typeColumns = new HashMap<>();
        for (Domain domain : localDomains) {
            typeColumns.put(domain.id(), new HashIDSet(domain.columns()));
        }

        ConcurrentLinkedQueue<Domain> queue;
        queue = new ConcurrentLinkedQueue<>(localDomains.toList());
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            DomainFrequencyEstimator command;
            command = new DomainFrequencyEstimator(
                    queue,
                    localDomains,
                    minSupportConstraint,
                    helper,
                    typeColumns
            );
            es.execute(command);
        }
        es.shutdown();
        es.awaitTermination(threads, TimeUnit.DAYS);

        IdentifiableCounterSet frequencyEstimate = new IdentifiableCounterSet();
        for (int domainId : typeColumns.keySet()) {
            frequencyEstimate.add(domainId, typeColumns.get(domainId).length() - 1);
        }
        
        if (verbose) {
            System.out.println(
                    "GOT FREQUENCY FOR " + localDomains.length() +
                    " DOMAINS @ " + new java.util.Date()
            );
        }

        // Compte support for all local domains. Maintain domains that are
        // identified as strong domains in a domain buffer together with the
        // local domains that provided support.
        HashObjectSet<DomainSupport> strongDomains = new HashObjectSet<>();
        queue = new ConcurrentLinkedQueue<>(localDomains.toList());

        es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            DomainSupportComputer command;
            command = new DomainSupportComputer(
                    queue,
                    localDomains,
                    domainOverlapConstraint,
                    supportFraction,
                    frequencyEstimate,
                    helper,
                    verbose,
                    strongDomains
            );
            es.execute(command);
        }
        es.shutdown();
        es.awaitTermination(threads, TimeUnit.DAYS);
        
        // For each strong domain generate the set of local domains that
        // provided the support for each other to become a strong domain.
        UndirectedConnectedComponents supportGraph;
        supportGraph = new UndirectedConnectedComponents(strongDomains.keys());
        
        for (DomainSupport strongDomain : strongDomains) {
            for (int supportDomId : strongDomain.supportSet()) {
                if (strongDomains.contains(supportDomId)) {
                    supportGraph.edge(strongDomain.id(), supportDomId);
                }
            }
        }
        
        StrongDomainWriter consumer;
        consumer = new StrongDomainWriter(outputFile, localDomains);
        
        consumer.open();

        for (IdentifiableIDSet strongDomain : supportGraph.getComponents()) {
            consumer.consume(strongDomain);
        }
        
        consumer.close();
        
        
        Date end = new Date();
        if (verbose) {
            System.out.println("DONE @ " + end);
            long execTime = end.getTime() - start.getTime();
            _telemetry.add(TELEMETRY_ID, execTime);
        }
    }
}
