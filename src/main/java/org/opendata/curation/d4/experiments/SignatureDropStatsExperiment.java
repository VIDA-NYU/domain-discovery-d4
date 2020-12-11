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
package org.opendata.curation.d4.experiments;

import org.opendata.curation.d4.signature.*;
import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
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
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQIndex;

/**
 * Experiment to evaluate the impact of the last drop and full signature
 * constraint on signature blocks generation.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureDropStatsExperiment {
    
    private class BlockGeneratorTask implements Runnable {

        private final MaxDropFinder<SignatureValue> _candidateFinder;
        private final EQIndex _eqIndex;
        private final ConcurrentLinkedQueue<Integer> _queue;
        private final ContextSignatureGenerator _sigFact;
        private final SynchronizedWriter _writer;
        
        public BlockGeneratorTask(
                EQIndex eqIndex,
                ConcurrentLinkedQueue<Integer> queue,
                ContextSignatureGenerator sigFact,
                MaxDropFinder<SignatureValue> candidateFinder,
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
                int pruneIndex = _candidateFinder.getPruneIndex(sig);
                double rightBound;
                if (pruneIndex == sig.size()) {
                    rightBound = 0;
                } else {
                    rightBound = sig.get(pruneIndex).value();
                }
                double drop = sig.get(pruneIndex - 1).value() - rightBound;
                double lastValue = sig.get(sig.size() - 1).value();
                boolean lastDropIsGreater = drop < lastValue;
                double fullSigDiff = sig.get(0).value() - lastValue;
                boolean isFullSig = fullSigDiff < lastValue;
                String type = null;
                if (isFullSig) {
                    type = "F";
                } else if (lastDropIsGreater) {
                    type = "L";
                }
                if (type != null) {
                    EQ eq = _eqIndex.get(nodeId);
                    String line = String.format(
                            "%d\t%s\t%f\t%d\t%d",
                            nodeId,
                            type,
                            sig.get(0).value(),
                            eq.columns().length(),
                            sig.size()
                    );
                    _writer.write(line);
                }
            }
        }
    }

    public void run(
            EQIndex eqIndex,
            ContextSignatureGenerator sigFact,
            ConcurrentLinkedQueue<Integer> queue,
            MaxDropFinder<SignatureValue> candidateFinder,
            int threads,
            SynchronizedWriter writer
    ) throws java.lang.InterruptedException, java.io.IOException {

        Date start = new Date();
        System.out.println("START @ " + start);
        
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
        System.out.println("END @ " + end);
    }

    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <threads>\n" +
            "  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(SignatureDropStatsExperiment.class.getName());
    
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
            new SignatureDropStatsExperiment().run(
                    eqIndex,
                    new ContextSignatureGenerator(eqIndex.nodes()),
                    new ConcurrentLinkedQueue<>(eqIndex.keys().toList()),
                    candidateFinder,
                    threads,
                    new SynchronizedWriter(out)
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
