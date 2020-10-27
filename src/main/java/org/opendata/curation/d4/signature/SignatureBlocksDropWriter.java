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
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.io.FileSystem;
import org.opendata.core.io.SynchronizedWriter;
import org.opendata.core.prune.CandidateSetFinder;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.IDSet;
import org.opendata.db.eq.EQIndex;

/**
 * Generate output file containing information about the steepest drops in
 * context signatures.
 * 
 * The output contains a single tab-delimited line for each equivalence class
 * containing the following information:
 * 
 * - equivalence class identifier
 * - similarity of first context signature entry
 * - list of similarities for nodes where the steepest drop occurs (separated
 *   by ':'.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksDropWriter {
    
    private class SignatureDrop {
        
        private final int _blockLength;
        private final int _columnCount;
        private final BigDecimal _firstElement;
        private final BigDecimal _lastElement;
        
        public SignatureDrop(BigDecimal first, BigDecimal last, int blockLength, int columnCount) {
            
            _firstElement = first;
            _lastElement = last;
            _blockLength = blockLength;
            _columnCount = columnCount;
        }
        
        @Override
        public String toString() {
            
            return String.format(
                    "%s-%s:%d:%d",
                    _firstElement.setScale(2, RoundingMode.HALF_DOWN).toPlainString(),
                    _lastElement.setScale(2, RoundingMode.HALF_DOWN).toPlainString(),
                    _blockLength,
                    _columnCount
            );
        }
    }
    
    private class BlockGeneratorTask implements Runnable {

        private final CandidateSetFinder<SignatureValue> _candidateFinder;
        private final EQIndex _eqIndex;
        private final ConcurrentLinkedQueue<Integer> _queue;
        private final ContextSignatureGenerator _sigFact;
        private final SynchronizedWriter _writer;
        
        public BlockGeneratorTask(
                EQIndex eqIndex,
                ConcurrentLinkedQueue<Integer> queue,
                ContextSignatureGenerator sigFact,
                CandidateSetFinder<SignatureValue> candidateFinder,
                SynchronizedWriter writer
        ) {
            _eqIndex = eqIndex;
            _queue = queue;
            _sigFact = sigFact;
            _candidateFinder = candidateFinder;
            _writer = writer;
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
                ArrayList<SignatureDrop> drops = new ArrayList<>();
                while (start < end) {
                    int pruneIndex = _candidateFinder.getPruneIndex(sig, start);
                    if (pruneIndex <= start) {
                        break;
                    }
                    int blockLen = pruneIndex - start;
                    IDSet columns = _eqIndex.get(nodeId).columns();
                    for (int iEl = start; iEl < pruneIndex; iEl++) {
                        int memberId = sig.get(iEl).id();
                        columns = columns.intersect(_eqIndex.get(memberId).columns());
                        if (columns.isEmpty()) {
                            break;
                        }
                    }
                    drops.add(
                            new SignatureDrop(
                                    sig.get(start).toBigDecimal(),
                                    sig.get(pruneIndex - 1).toBigDecimal(),
                                    blockLen,
                                    columns.length()
                            )
                    );
                    start = pruneIndex;
                }
                if (drops.isEmpty()) {
                    continue;
                }
                String line = nodeId + "\t" + drops.get(0).toString();
                for (int iDrop = 1; iDrop < drops.size(); iDrop++) {
                    line += "|" + drops.get(iDrop).toString();
                }
                _writer.write(line);
            }
        }
    }

    public void run(
            EQIndex eqIndex,
            ContextSignatureGenerator sigFact,
            ConcurrentLinkedQueue<Integer> queue,
            CandidateSetFinder<SignatureValue> candidateFinder,
            int threads,
            boolean verbose,
            SynchronizedWriter writer
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
                            eqIndex,
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
        
        Date end = new Date();
        if (verbose) {
            System.out.println("END @ " + end);
        }
    }

    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <threads>\n" +
            "  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(SignatureBlocksDropWriter.class.getName());
    
    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        int threads = Integer.parseInt(args[1]);
        File outputFile = new File(args[2]);
        
        boolean fullSignatureConstraint = false;
        boolean ignoreLastDrop = true;

        MaxDropFinder<SignatureValue> candidateFinder;
        candidateFinder = new MaxDropFinder<>(
                new GreaterThanConstraint(BigDecimal.ZERO),
                fullSignatureConstraint,
                ignoreLastDrop
        );

        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            EQIndex eqIndex = new EQIndex(eqFile);
            new SignatureBlocksDropWriter().run(
                    eqIndex,
                    new ContextSignatureGenerator(eqIndex.nodes()),
                    new ConcurrentLinkedQueue<>(eqIndex.keys().toList()),
                    candidateFinder,
                    threads,
                    true,
                    new SynchronizedWriter(out)
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
