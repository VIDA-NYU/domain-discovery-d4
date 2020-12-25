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
package org.opendata.performance.overlap;

import java.io.BufferedReader;
import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.io.FileSystem;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.util.MemUsagePrinter;
import org.opendata.curation.d4.signature.BlockGeneratorTask;
import org.opendata.curation.d4.signature.ContextSignatureGenerator;
import org.opendata.curation.d4.signature.SignatureBlocksGenerator;
import org.opendata.curation.d4.signature.SignatureBlocksStats;
import org.opendata.curation.d4.signature.SignatureValue;
import org.opendata.db.eq.similarity.EQSimilarity;

/**
 * Evaluate the run time impact of different representations for equivalence
 * classes in JIEQSimilarity on the computation of column set overlaps.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class OverlapComputationExperiment {
    
    public void run(File eqFile, int runs) throws java.io.IOException {
        
        // Read the equivalence class file once to get the maximum identifier.
        List<Integer> nodes = new ArrayList<>();
        int maxId = -1;
        try (BufferedReader in = FileSystem.openReader(eqFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                int eqId = Integer.parseInt(line.split("\t")[0]);
                nodes.add(eqId);
                if (eqId > maxId) {
                    maxId = eqId;
                }
            }
        }

        String[] sims = new String[]{"HASH-ARRAY", "ARRAY-ARRAY", "LARGE-INNER", "LARGE-OUTER"};
        long[] execTimes = new long[sims.length];
        long[] sums = new long[sims.length];
        
        int iSim = 0;
        for (String simId : sims) {
            EQSimilarity simFunc = null;
            if (simId.equals("ARRAY-ARRAY")) {
                simFunc = JISimilarityArray.load(eqFile, maxId);
            } else if (simId.equals("HASH-ARRAY")) {
                simFunc = JISimilarityLookupArray.load(eqFile);
            } else if (simId.equals("LARGE-INNER")) {
                simFunc = JISimilarityHash.load(eqFile);
            } else if (simId.equals("LARGE-OUTER")) {
                simFunc = JISimilarityHashLargeOuter.load(eqFile);
            }
        
            MaxDropFinder<SignatureValue> candidateFinder;
            candidateFinder = new MaxDropFinder<>(
                    new GreaterThanConstraint(BigDecimal.ZERO),
                    true,
                    false
            );

            new MemUsagePrinter().print();

            for (int i = 0; i < runs; i++) {
                long start = new Date().getTime();
                SignatureBlocksStats consumer = new SignatureBlocksStats();
                consumer.open();
                new BlockGeneratorTask(
                        new ConcurrentLinkedQueue<>(nodes),
                        new ContextSignatureGenerator(nodes, simFunc),
                        new SignatureBlocksGenerator(candidateFinder, false),
                        consumer
                ).run();
                consumer.close();
                long end = new Date().getTime();
                if (i > 0) {
                    execTimes[iSim] += (end - start);
                    sums[iSim] += consumer.sum();
                }
                System.out.println(String.format("%d\t%s\t%d (ms)", i, simId, (end - start)));
            }
            iSim++;
            new MemUsagePrinter().print();
        }
        
        for (int i = 0; i < execTimes.length; i++) {
            System.out.println(String.format("%s\t%.4f\t%.4f", sims[i], ((double)execTimes[i] / (runs - 1)), ((double)sums[i] / (runs - 1))));
        }
    }
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println("Usage: <eq-file> <runs>");
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        int runs = Integer.parseInt(args[1]);
        
        try {
            new OverlapComputationExperiment().run(eqFile, runs);
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
