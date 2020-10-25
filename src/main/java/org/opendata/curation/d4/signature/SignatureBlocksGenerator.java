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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.prune.CandidateSetFinder;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.prune.ThresholdFinder;
import org.opendata.db.eq.EQIndex;

/**
 * Generate output file containing context signature blocks.
 * 
 * The output contains a single tab-delimited line for each equivalence class
 * containing the following information:
 * 
 * - equivalence class identifier
 * - similarity of first context signature entry
 * - list of signature blocks. Each block is a comma-separated list of node
 *   identifier. Blocks are separated by a tab.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksGenerator {
    
    public static final String MAX_DROP = "MAX-DROP";
    public static final String THRESHOLD = "THRESHOLD";

    public static final String TELEMETRY_ID = "SIGNATURE BLOCKS";
    
    public static final String DEFAULT = MAX_DROP;
    
    private class BlockGeneratorTask implements Runnable {

        private final CandidateSetFinder<SignatureValue> _candidateFinder;
        private final SignatureBlocksConsumer _consumer;
        private final ConcurrentLinkedQueue<Integer> _queue;
        private final ContextSignatureGenerator _sigFact;
        
        public BlockGeneratorTask(
                ConcurrentLinkedQueue<Integer> queue,
                ContextSignatureGenerator sigFact,
                CandidateSetFinder<SignatureValue> candidateFinder,
                SignatureBlocksConsumer consumer
        ) {
            
            _queue = queue;
            _sigFact = sigFact;
            _candidateFinder = candidateFinder;
            _consumer = consumer;
        }
        
        @Override
        public void run() {

            Integer nodeId;
            while ((nodeId = _queue.poll()) != null) {
                List<SignatureValue> sig;
                sig = _sigFact.getSignature(nodeId).rankedElements();
                // No output if the context signautre is empty
                if (sig.isEmpty()) {
                    continue;
                }
                int start = 0;
                final int end = sig.size();
                ArrayList<int[]> blocks = new ArrayList<>();
                while (start < end) {
                    int pruneIndex = _candidateFinder.getPruneIndex(sig, start);
                    if (pruneIndex <= start) {
                        break;
                    }
                    int[] block = new int[pruneIndex - start];
                    for (int iEl = start; iEl < pruneIndex; iEl++) {
                        block[iEl - start] = sig.get(iEl).id();
                    }
                    Arrays.sort(block);
                    blocks.add(block);
                    start = pruneIndex;
                }
                _consumer.consume(
                        new SignatureBlocksImpl(
                                nodeId,
                                sig.get(0).toBigDecimal(),
                                blocks
                        )
                );
            }
        }
    }

    private final TelemetryCollector _telemetry;
    
    public SignatureBlocksGenerator(TelemetryCollector telemetry) {
        
        _telemetry = telemetry;
    }
    
    public SignatureBlocksGenerator() {
        
        this(new TelemetryPrinter());
    }
    
    private void compute(
            ContextSignatureGenerator sigFact,
            ConcurrentLinkedQueue<Integer> queue,
            CandidateSetFinder<SignatureValue> candidateFinder,
            int threads,
            boolean verbose,
            SignatureBlocksConsumerFactory consumerFactory
    ) throws java.lang.InterruptedException, java.io.IOException {

        if (verbose) {
            System.out.println(
                    String.format(
                            "SIGNATURE BLOCKS FOR %d EQs USING:\n" +
                            "  --threads=%d",
                            queue.size(),
                            threads
                    )
            );
        }
        
        Date start = new Date();
        if (verbose) {
            System.out.println("START @ " + start);
        }
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(
                    new BlockGeneratorTask(
                            queue,
                            sigFact,
                            candidateFinder,
                            consumerFactory.getConsumer()
                    )
            );
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        
        consumerFactory.close();
        
        Date end = new Date();
        if (verbose) {
            System.out.println("END @ " + end);
        }
        
        if (verbose) {
            long execTime = end.getTime() - start.getTime();
            _telemetry.add(TELEMETRY_ID, execTime);
        }
    }
    /**
     * Generate signature blocks using a fixed threshold constraint.
     * 
     * @param eqIndex
     * @param queue
     * @param threshold
     * @param threads
     * @param verbose
     * @param consumerFactory
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException 
     */
    public void runWithThreshold(
            EQIndex eqIndex,
            ConcurrentLinkedQueue<Integer> queue,
            Threshold threshold,
            int threads,
            boolean verbose,
            SignatureBlocksConsumerFactory consumerFactory
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        ThresholdFinder<SignatureValue> candidateFinder;
        candidateFinder = new ThresholdFinder<>(threshold);

        this.compute(
                new ContextSignatureGenerator(eqIndex.nodes()),
                queue,
                candidateFinder,
                threads,
                verbose,
                consumerFactory
        );
    }
    
    /**
     * Generate signature blocks using consecutive steepest drops.
     * 
     * @param eqIndex
     * @param queue
     * @param fullSignatureConstraint
     * @param ignoreLastDrop
     * @param threads
     * @param verbose
     * @param consumerFactory
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException 
     */
    public void runWithMaxDrop(
            EQIndex eqIndex,
            ConcurrentLinkedQueue<Integer> queue,
            boolean fullSignatureConstraint,
            boolean ignoreLastDrop,
            int threads,
            boolean verbose,
            SignatureBlocksConsumerFactory consumerFactory
    ) throws java.lang.InterruptedException, java.io.IOException {

        MaxDropFinder<SignatureValue> candidateFinder;
        candidateFinder = new MaxDropFinder<>(
                new GreaterThanConstraint(BigDecimal.ZERO),
                fullSignatureConstraint,
                ignoreLastDrop
        );

        this.compute(
                new ContextSignatureGenerator(eqIndex.nodes()),
                queue,
                candidateFinder,
                threads,
                verbose,
                consumerFactory
        );
    }
}
