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

import java.io.File;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksGenerator;
import org.opendata.curation.d4.signature.SignatureBlocksIndex;
import org.opendata.curation.d4.signature.trim.LiberalTrimmer;
import org.opendata.curation.d4.telemetry.TelemetrySet;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

/**
 * Print execution time and signature sizes for context signature blocks
 * generation for all columns in a database.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureComputationExperiment {
    
    private final static String COMMAND =
            "Usage: <eq-file> <threads> <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(SignatureComputationExperiment.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        int threads = Integer.parseInt(args[1]);
        File outFile = new File(args[2]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outFile)) {
            EQIndex eqIndex = new EQIndex(eqFile);
            for (Column column : eqIndex.columns()) {
                TelemetrySet telemetry = new TelemetrySet();
                SignatureBlocksIndex consumer = new SignatureBlocksIndex();
                new SignatureBlocksGenerator(telemetry)
                        .runWithMaxDrop(
                                eqIndex,
                                new ConcurrentLinkedQueue<>(column.toList()),
                                threads,
                                true,
                                new LiberalTrimmer(eqIndex.nodeSizes(), consumer)
                        );
                long execTime = telemetry.get(SignatureBlocksGenerator.TELEMETRY_ID);
                int blockCount = 0;
                int nodeCount = 0;
                for (SignatureBlocks sig : consumer) {
                    blockCount += sig.size();
                    for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
                        nodeCount += sig.get(iBlock).length;
                    }
                }
                String line = String.format(
                        "%d\t%d\t%d\t%d\t%d",
                        column.id(),
                        execTime,
                        consumer.length(),
                        blockCount,
                        nodeCount
                );
                out.println(line);
                System.out.println(line);
            }
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
        }
    }
}
