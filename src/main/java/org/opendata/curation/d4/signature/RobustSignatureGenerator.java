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
package org.opendata.curation.d4.signature;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.db.eq.similarity.EQSimilarity;

/**
 * Generate output file containing robust context signature blocks.
 * 
 * The output contains a single tab-delimited line for each equivalence class
 * containing the following information:
 * 
 * - equivalence class identifier
 * - list of signature blocks. Each block is prefixed by the similarity of the
 *   first and last element (delimited by '-') and a a comma-separated list of
 *   node identifier (delimited from the prefix by ':'). Blocks are separated
 *   by a tab.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class RobustSignatureGenerator {
    
    public static final String TELEMETRY_ID = "SIGNATURE BLOCKS";
    
    private final TelemetryCollector _telemetry;
    
    public RobustSignatureGenerator(TelemetryCollector telemetry) {
        
        _telemetry = telemetry;
    }
    
    public RobustSignatureGenerator() {
        
        this(new TelemetryPrinter());
    }
    
    /**
     * Generate signature blocks using consecutive steepest drops.
     * 
     * @param eqIdentifiers
     * @param simFunc
     * @param fullSignatureConstraint
     * @param ignoreLastDrop
     * @param ignoreMinorDrop
     * @param threads
     * @param verbose
     * @param consumer
     * @throws java.lang.InterruptedException
     */
    public void run(
            Collection<Integer> eqIdentifiers,
            EQSimilarity simFunc,
            boolean fullSignatureConstraint,
            boolean ignoreLastDrop,
            boolean ignoreMinorDrop,
            int threads,
            boolean verbose,
            SignatureBlocksConsumer consumer
    ) throws java.lang.InterruptedException {

        if (verbose) {
            System.out.println(String.format(
                    "SIGNATURE BLOCKS FOR %d EQs", eqIdentifiers.size())
            );
        }
        
        MaxDropFinder<SignatureValue> candidateFinder;
        candidateFinder = new MaxDropFinder<>(
                new GreaterThanConstraint(BigDecimal.ZERO),
                fullSignatureConstraint,
                ignoreLastDrop
        );

        ContextSignatureGenerator sigFact;
        sigFact = new ContextSignatureGenerator(eqIdentifiers, simFunc);

        ConcurrentLinkedQueue queue;
        queue = new ConcurrentLinkedQueue<>(eqIdentifiers);
        
        Date start = new Date();
        if (verbose) {
            System.out.println("START @ " + start);
        }
        
        consumer.open();
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(
                    new BlockGeneratorTask(
                            queue,
                            sigFact,
                            new SignatureBlocksGenerator(
                                    candidateFinder,
                                    ignoreMinorDrop
                            ),
                            consumer
                    )
            );
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        
        consumer.close();
        
        Date end = new Date();
        if (verbose) {
            System.out.println("END @ " + end);
        }
        
        if (verbose) {
            long execTime = end.getTime() - start.getTime();
            _telemetry.add(TELEMETRY_ID, execTime);
        }
    }
}
