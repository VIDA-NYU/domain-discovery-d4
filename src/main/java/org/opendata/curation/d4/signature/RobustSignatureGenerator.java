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
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.core.constraint.GreaterThanConstraint;
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
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <trimmer> [LIBERAL | COLSUPP]\n" +
            "  <full-signature-constraint>\n" +
            "  <ignore-last-drop>\n" +
            "  <ignore-minor-drop>\n" +
            "  <node-id>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(RobustSignatureGenerator.class.getName());
    
    public static void main(String[] args) throws IOException {
        
        if (args.length != 7) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        String trimmerSpec = args[1].toUpperCase();
        boolean fullSignatureConstraint = Boolean.parseBoolean(args[2]);
        boolean ignoreLastDrop = Boolean.parseBoolean(args[3]);
        boolean ignoreMinorDrop = Boolean.parseBoolean(args[4]);
        int nodeId = Integer.parseInt(args[5]);
        File outputFile = new File(args[6]);
        
        ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<>();
        queue.add(nodeId);
        
        try {
            new RobustSignatureGenerator().run(
                    new EQIndex(eqFile),
                    queue,
                    trimmerSpec,
                    fullSignatureConstraint,
                    ignoreLastDrop,
                    ignoreMinorDrop,
                    1,
                    true,
                    new SignatureBlocksWriter(outputFile)
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
