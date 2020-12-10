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
import org.opendata.core.prune.CandidateSetFinder;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.curation.d4.signature.trim.ColumnSupportBlockFilter;
import org.opendata.curation.d4.signature.trim.LiberalRobustifier;
import org.opendata.curation.d4.signature.trim.SignatureRobustifier;
import org.opendata.db.eq.EQIndex;

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

        private final CandidateSetFinder<SignatureValue> _candidateFinder;
        private final SignatureBlocksConsumer _consumer;
        private final boolean _ignoreMinorDrop;
        private final ConcurrentLinkedQueue<Integer> _queue;
        private final ContextSignatureGenerator _sigFact;
        
        public BlockGeneratorTask(
                ConcurrentLinkedQueue<Integer> queue,
                ContextSignatureGenerator sigFact,
                CandidateSetFinder<SignatureValue> candidateFinder,
                boolean ignoreMinorDrop,
                SignatureBlocksConsumer consumer
        ) {
            
            _queue = queue;
            _sigFact = sigFact;
            _candidateFinder = candidateFinder;
            _ignoreMinorDrop = ignoreMinorDrop;
            _consumer = consumer;
        }
        
        private SignatureBlock getBlock(List<SignatureValue> sig, int start, int end) {
            
            int[] block = new int[end - start];
            for (int iEl = start; iEl < end; iEl++) {
                block[iEl - start] = sig.get(iEl).id();
            }
            Arrays.sort(block);
            return new SignatureBlock(
                    block,
                    sig.get(start).value(),
                    sig.get(end - 1).value()
            );
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
                ArrayList<SignatureBlock> blocks = new ArrayList<>();
                int start = 0;
                final int end = sig.size();
                while (start < end) {
                    int pruneIndex = _candidateFinder.getPruneIndex(sig, start);
                    if (pruneIndex <= start) {
                        break;
                    }
                    // If the ignoreMinorDrop flag is true check that the
                    // difference at the drop is at least as large as the
                    // difference between the elements in the block.
                    if (_ignoreMinorDrop) {
                        double rightBound = 0;
                        if (sig.size() < pruneIndex) {
                            rightBound = sig.get(pruneIndex).value();
                        }
                        double leftBound = sig.get(pruneIndex - 1).value();
                        double diff = leftBound - rightBound;
                        double blockDiff = sig.get(start).value() - leftBound;
                        if (blockDiff > diff) {
                            // We encountered a minor drop. If the list of
                            // blocks is empty (i.e., there is no steepest drop
                            // but the full signature constrant is not satisfied
                            // either) we break to return an empty signature.
                            // Otherwise, we add the remaining elements as the
                            // final block.
                            if (blocks.isEmpty()) {
                                break;
                            } else {
                                pruneIndex = end;
                            }
                        }
                    }
                    blocks.add(this.getBlock(sig, start, pruneIndex));
                    start = pruneIndex;
                }
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
     * @param eqIndex
     * @param queue
     * @param robustifierSpec
     * @param fullSignatureConstraint
     * @param ignoreLastDrop
     * @param ignoreMinorDrop
     * @param threads
     * @param verbose
     * @param writer
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException 
     */
    public void run(
            EQIndex eqIndex,
            ConcurrentLinkedQueue<Integer> queue,
            String robustifierSpec,
            boolean fullSignatureConstraint,
            boolean ignoreLastDrop,
            boolean ignoreMinorDrop,
            int threads,
            boolean verbose,
            SignatureBlocksWriter writer
    ) throws java.lang.InterruptedException, java.io.IOException {

        if (verbose) {
            System.out.println(
                    String.format(
                            "SIGNATURE BLOCKS FOR %d EQs USING:\n" +
                            "  --eqs=%s\n" +
                            "  --robustifier=%s\n" +
                            "  --fullSignatureConstraint=%s\n" +
                            "  --ignoreLastDrop=%s\n" +
                            "  --ignoreMinorDrop=%s\n" +
                            "  --threads=%d\n" +
                            "  --signatures=%s",
                            queue.size(),
                            eqIndex.source(),
                            robustifierSpec,
                            Boolean.toString(fullSignatureConstraint),
                            Boolean.toString(ignoreLastDrop),
                            Boolean.toString(ignoreMinorDrop),
                            threads,
                            writer.target()
                    )
            );
        }
        

        MaxDropFinder<SignatureValue> candidateFinder;
        candidateFinder = new MaxDropFinder<>(
                new GreaterThanConstraint(BigDecimal.ZERO),
                fullSignatureConstraint,
                ignoreLastDrop
        );

        SignatureRobustifier consumer;
        if (robustifierSpec.equalsIgnoreCase(SignatureRobustifier.COLSUPP)) {
            consumer = new ColumnSupportBlockFilter(eqIndex, writer);
        } else if (robustifierSpec.equalsIgnoreCase(SignatureRobustifier.LIBERAL)) {
            consumer = new LiberalRobustifier(eqIndex.nodeSizes(), writer);
        } else {
            throw new IllegalArgumentException(
                    String.format("Unknown robustifier '%s'", robustifierSpec)
            );
        }
        
        ContextSignatureGenerator sigFact;
        sigFact = new ContextSignatureGenerator(eqIndex.nodes());

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
                            candidateFinder,
                            ignoreMinorDrop,
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
