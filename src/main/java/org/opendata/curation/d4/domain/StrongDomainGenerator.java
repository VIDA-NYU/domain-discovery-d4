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
import org.opendata.core.constraint.Threshold;
import org.opendata.core.set.HashIDSet;
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
     * Estimate frequency for the semantic type of a local domain based on the
     * number of columns that contain overlapping local domains for a given
     * threshold.
     */
    private class DomainFrequencyEstimator implements Runnable {

        private final List<Domain> _domains;
        private final DomainHelper _helper;
        private final ConcurrentLinkedQueue<Domain> _queue;
        private final Threshold _supportConstraint;
        private final HashMap<Integer, HashIDSet> _typeColumns;

        public DomainFrequencyEstimator(
                ConcurrentLinkedQueue<Domain> queue,
                List<Domain> domains,
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
     * Compute support for a local domain based on overlap with domains in 
     * columns that where estimated to contain the same semantic type.
     */
    private class DomainSupportComputer implements Runnable {

        private final DomainConsumer _consumer;
        private final List<Domain> _domains;
        private final IdentifiableCounterSet _frequencyEstimate;
        private final DomainHelper _helper;
        private final Threshold _overlapConstraint;
        private final ConcurrentLinkedQueue<Domain> _queue;
        private final BigDecimal _supportFraction;
        private final boolean _verbose;
        
        public DomainSupportComputer(
                ConcurrentLinkedQueue<Domain> queue,
                List<Domain> domains,
                Threshold overlapConstraint,
                BigDecimal supportFraction,
                IdentifiableCounterSet frequencyEstimate,
                DomainHelper helper,
                boolean verbose,
                DomainConsumer consumer
        ) {
            _queue = queue;
            _domains = domains;
            _overlapConstraint = overlapConstraint;
            _supportFraction = supportFraction;
            _frequencyEstimate = frequencyEstimate;
            _helper = helper;
            _verbose = verbose;
            _consumer = consumer;
        }
        
        @Override
        public void run() {

            Domain domain;
            while ((domain = _queue.poll()) != null) {
                int frequency = _frequencyEstimate.get(domain.id()).value();
                int minColumnCount = ((int)Math.floor(
                        new BigDecimal(frequency)
                            .multiply(_supportFraction)
                            .doubleValue()
                )) + 1;
                boolean added = false;
                if (domain.columns().length() > minColumnCount) {
                    _consumer.consume(domain);
                    added = true;
                } else {
                    //int edgeCount = domain.columns().length() - 1;
                    HashIDSet columns = new HashIDSet(domain.columns());
                    for (Domain domI : _domains) {
                        if (domain.id() != domI.id()) {
                            if (domain.overlaps(domI)) {
                                BigDecimal ji = _helper.termOverlap(domain, domI);
                                if (_overlapConstraint.isSatisfied(ji)) {
                                    columns.add(domI.columns());
                                    int edgeCount = columns.length() - 1;
                                    if (edgeCount >= minColumnCount) {
                                        _consumer.consume(domain);
                                        added = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                if (_verbose) {
                    System.out.println(domain.id() + "\t" + domain.columns().length() + "\t" + frequency + "\t" + minColumnCount + "\t" + added);
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
     * @param localDomains
     * @param domainOverlapConstraint
     * @param minSupportConstraint
     * @param supportFraction
     * @param verbose
     * @param threads
     * @param consumer
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException 
     */
    public void run(
            EQIndex nodes,
            List<Domain> localDomains,
            Threshold domainOverlapConstraint,
            Threshold minSupportConstraint,
            BigDecimal supportFraction,
            boolean verbose,
            int threads,
            DomainConsumer consumer
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        Date start = new Date();
        if (verbose) {
            System.out.println("START WITH " + localDomains.size() + " DOMAINS @ " + start);
        }
        
        DomainHelper helper = new DomainHelper(nodes, localDomains);
        
        // For each local domain we first estimate the frequency of the semantic
        // type that the domain represents in the database. The estimate is
        // based on the number of columns that contain a local domain that
        // overlaps with the given domain at a low (minSupportConstraint)
        // threshold.

        // For each local domain we compute the set of columns that contain a
        // local domain that (potentially) is of the same semantic type.
        HashMap<Integer, HashIDSet> typeColumns = new HashMap<>();
        for (Domain domain : localDomains) {
            typeColumns.put(domain.id(), new HashIDSet(domain.columns()));
        }

        ConcurrentLinkedQueue<Domain> queue;
        queue = new ConcurrentLinkedQueue<>(localDomains);
        
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
            System.out.println("GOT SUPPORT FOR " + localDomains.size() + " DOMAINS @ " + new java.util.Date());
        }

        queue = new ConcurrentLinkedQueue<>(localDomains);

        consumer.open();
        
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
                    false,
                    consumer
            );
            es.execute(command);
        }
        es.shutdown();
        es.awaitTermination(threads, TimeUnit.DAYS);
        
        consumer.close();
        
        Date end = new Date();
        if (verbose) {
            System.out.println("DONE @ " + end);
        }
        
        long execTime = end.getTime() - start.getTime();
        _telemetry.add(TELEMETRY_ID, execTime);
    }
    
    private static final String ARG_DOMAINOVERLAP = "domainOverlap";
    private static final String ARG_SUPPORTFRAC = "supportFraction";
    private static final String ARG_THREADS = "threads";

    private static final String[] ARGS = {
        ARG_DOMAINOVERLAP,
        ARG_SUPPORTFRAC,
        ARG_THREADS
    };
    
    private static final String COMMAND =
            "Usage\n" +
            "  --" + ARG_DOMAINOVERLAP + "=<constraint> [default: GT0.5]\n" +
            "  --" + ARG_SUPPORTFRAC + "=<real> [default: 0.25]\n" +
            "  --" + ARG_THREADS + "=<int> [default: 6]\n" +
            "  <eq-file>\n" +
            "  <local-domain-file>\n" +
            "  <ouput-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(StrongDomainGenerator.class.getName());
    
    public static void main(String[] args) {
        
	System.out.println(Constants.NAME + " - Strong Domain Generator - Version (" + Constants.VERSION + ")\n");

        if (args.length < 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Arguments params = new Arguments(ARGS, args, 3);
        File eqFile = new File(params.fixedArg(0));
        File domainFile = new File(params.fixedArg(1));
        File outputFile = new File(params.fixedArg(2));

        Threshold domainOverlapConstraint = Threshold
                .getConstraint(params.getAsString(ARG_DOMAINOVERLAP, "GT0.5"));
        BigDecimal supportFraction = params.getAsBigDecimal(ARG_SUPPORTFRAC, new BigDecimal("0.25"));
        int threads = params.getAsInt(ARG_THREADS, 6);
        
        try {
            new StrongDomainGenerator().run(
                    new EQIndex(eqFile),
                    new DomainReader(domainFile).read().toList(),
                    domainOverlapConstraint,
                    Threshold.getConstraint("GT0.1"),
                    supportFraction,
                    true,
                    threads,
                    new DomainWriter(outputFile)
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
