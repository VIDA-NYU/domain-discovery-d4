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
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.prune.MaxDropFinder;

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
    
    /**
     * Worker for generating signature blocks for equivalence classes.
     * 
     */
    private class BlockGeneratorTask implements Runnable {

        private final SignatureBlocksGenerator _blockGenerator;
        private final SignatureBlocksConsumer _consumer;
        private final ConcurrentLinkedQueue<Integer> _queue;
        private final ContextSignatureGenerator _sigFact;
        
        public BlockGeneratorTask(
                ConcurrentLinkedQueue<Integer> queue,
                ContextSignatureGenerator sigFact,
                SignatureBlocksGenerator blockGenerator,
                SignatureBlocksConsumer consumer
        ) {
            
            _queue = queue;
            _sigFact = sigFact;
            _blockGenerator = blockGenerator;
            _consumer = consumer;
        }
        
        @Override
        public void run() {

            Integer nodeId;
            while ((nodeId = _queue.poll()) != null) {
                List<SignatureBlock> blocks = _blockGenerator
                        .toBlocks(_sigFact.getSignature(nodeId).rankedElements());
                if (!blocks.isEmpty()) {
                    _consumer.consume(nodeId, blocks);
                }
            }
        }
    }

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
     * @param equivalenceClasses
     * @param sigFact
     * @param fullSignatureConstraint
     * @param ignoreLastDrop
     * @param ignoreMinorDrop
     * @param threads
     * @param verbose
     * @param consumer
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException
     */
    public void run(
            List<Integer> equivalenceClasses,
            ContextSignatureGenerator sigFact,
            boolean fullSignatureConstraint,
            boolean ignoreLastDrop,
            boolean ignoreMinorDrop,
            int threads,
            boolean verbose,
            SignatureBlocksConsumer consumer
    ) throws java.lang.InterruptedException, java.io.IOException {

        MaxDropFinder<SignatureValue> candidateFinder;
        candidateFinder = new MaxDropFinder<>(
                new GreaterThanConstraint(BigDecimal.ZERO),
                fullSignatureConstraint,
                ignoreLastDrop
        );

        ConcurrentLinkedQueue<Integer> queue;
        queue = new ConcurrentLinkedQueue<>(equivalenceClasses);
        
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
