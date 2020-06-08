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

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
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
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.prune.CandidateSetFinder;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.prune.ThresholdFinder;
import org.opendata.core.set.HashIDSet;
import org.opendata.curation.d4.signature.trim.LiberalTrimmer;
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

            int count = 0;
            
            Integer nodeId;
            while ((nodeId = _queue.poll()) != null) {
                count++;
                if ((count % 1000) == 0) {
                    System.out.println(count + " @ " + new java.util.Date());
                }
                SignatureBlocks sig = _sigFact
                		.getSignature(nodeId)
                		.toSignatureBlocks(_candidateFinder);
                _consumer.consume(sig);
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
            SignatureBlocksConsumer writer
    ) throws java.lang.InterruptedException, java.io.IOException {

        System.out.println(
                "WRITE CONTEXT SIGNATURE BLOCKS FOR " + queue.size() +
                " EQUIVALENCE CLASSES USING " + threads + " THREADS."
        );
        
        Date start = new Date();
        System.out.println("START @ " + start);
        
        writer.open();
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(
                    new BlockGeneratorTask(
                            queue,
                            sigFact,
                            candidateFinder,
                            writer
                    )
            );
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        
        writer.close();
        
        Date end = new Date();
        System.out.println("END @ " + end);
        
        long execTime = end.getTime() - start.getTime();
        _telemetry.add(TELEMETRY_ID, execTime);
    }

    /**
     * Generate signature blocks using a fixed threshold constraint.
     * 
     * @param eqIndex
     * @param queue
     * @param threshold
     * @param threads
     * @param writer
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException 
     */
    public void runWithThreshold(
            EQIndex eqIndex,
            ConcurrentLinkedQueue<Integer> queue,
            Threshold threshold,
            int threads,
            SignatureBlocksConsumer writer
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        ThresholdFinder<SignatureValue> candidateFinder;
        candidateFinder = new ThresholdFinder<>(threshold);

        this.compute(
                new ContextSignatureGenerator(eqIndex.nodes()),
                queue,
                candidateFinder,
                threads,
                writer
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
     * @param writer
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException 
     */
    public void runWithMaxDrop(
            EQIndex eqIndex,
            ConcurrentLinkedQueue<Integer> queue,
            boolean fullSignatureConstraint,
            boolean ignoreLastDrop,
            int threads,
            SignatureBlocksConsumer writer
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
                writer
        );
    }
    
    private static final String ARG_FULLSIG = "fullSigConstraint";
    private static final String ARG_LASTDROP = "ignoreLastDrop";
    private static final String ARG_NODES = "nodes";
    private static final String ARG_THREADS = "threads";
    private static final String ARG_THRESHOLD = "threshold";
    
    private static final String[] ARGS = {
        ARG_FULLSIG,
        ARG_LASTDROP,
        ARG_NODES,
        ARG_THREADS,
        ARG_THRESHOLD
    };
    
    private static final String COMMAND =
            "Usage\n" +
            "  --" + ARG_FULLSIG + "=[true | false] [default: false]\n" +
            "  --" + ARG_LASTDROP + "=[true | false] [default: true]\n" +
            "  --" + ARG_NODES + "=<node-list-file> [default: null]\n" +
            "  --" + ARG_THREADS + "=<int> [default: 6]\n" +
            "  --" + ARG_THRESHOLD + "=<constraint> [default: null]\n" +
            "  <eq-file>\n" +
            "  <output-file-or-directory>";
    
    private static final Logger LOGGER = Logger
            .getLogger(SignatureBlocksGenerator.class.getName());
    
    public static void main(String[] args) {
        
    	System.out.println(Constants.NAME + " - Signature Blocks Generator - Version (" + Constants.VERSION + ")\n");

        if (args.length < 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Arguments params = new Arguments(ARGS, args, 2);
        File eqFile = new File(params.fixedArg(0));
        File outputFile = new File(params.fixedArg(1));

        int threads = params.getAsInt(ARG_THREADS, 6);
        
        boolean fullSignatureConstraint = params.getAsBool(ARG_FULLSIG, false);
        boolean ignoreLastDrop = params.getAsBool(ARG_LASTDROP, true);
        
        Threshold threshold = null;
        if (params.has("threshold")) {
            if ((params.has("fullSigConstraint")) || (params.has("ignoreLastDrop"))) {
                throw new IllegalArgumentException("Illegal argument combination");
            }
            threshold = Threshold.getConstraint(params.get(ARG_THRESHOLD));
        }
        
        String nodes = null;
        if (params.has(ARG_NODES)) {
            nodes = params.get(ARG_NODES);
        }
        
        try {
            // Read the node index
            EQIndex nodeIndex = new EQIndex(eqFile);
            // Read the list of node identifier if a nodes file was given
            List<Integer> nodeFilter;
            if (nodes != null) {
                File nodesFile = new File(nodes);
                if (nodesFile.exists()) {
                    nodeFilter = new HashIDSet(nodesFile).toList();
                } else {
                    String[] interval = nodes.split("-");
                    int start = Integer.parseInt(interval[0]);
                    int end = Integer.parseInt(interval[1]);
                    nodeFilter = new ArrayList<>();
                    for (int iNode = start; iNode < end; iNode++) {
                        nodeFilter.add(iNode);
                    }
                }
            } else {
                nodeFilter = nodeIndex.keys().toList();
            }
            // Depending on whether the threshold constraint is given or not
            // we call the respective run method of the signature blocks
            // generator.
            LiberalTrimmer writer = new LiberalTrimmer(
                    nodeIndex.nodeSizes(),
                    new SignatureBlocksWriter(outputFile)
                );
            if (threshold != null) {
                new SignatureBlocksGenerator().runWithThreshold(
                        nodeIndex,
                        new ConcurrentLinkedQueue<>(nodeFilter),
                        threshold,
                        threads,
                        writer
                );
            } else {
                new SignatureBlocksGenerator().runWithMaxDrop(
                        nodeIndex,
                        new ConcurrentLinkedQueue<>(nodeFilter),
                        fullSignatureConstraint,
                        ignoreLastDrop,
                        threads,
                        writer
                );
            }
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
