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
package org.opendata.curation.d4.prov;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.signature.ConcurrentSignatureBlocksStream;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.trim.CentristTrimmer;
import org.opendata.curation.d4.signature.trim.PrecisionScore;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

/**
 * For each signature count the blocks (and their size) for those blocks that
 * are part of at least one centrist column signature.
 * 
 * @author heiko
 */
public class CentristBlockUsageWriter {
    
    private class OverlapComputer implements Runnable {

        private final EQIndex _eqIndex;
        private final File _outputFile;
        private final ConcurrentSignatureBlocksStream _signatures;
        private final HashMap<Integer, CentristTrimmer> _trimmer;
        public OverlapComputer(
                EQIndex eqIndex,
                HashMap<Integer, CentristTrimmer> trimmer,
                ConcurrentSignatureBlocksStream signatures,
                File outputFile
        ) {
            _eqIndex = eqIndex;
            _trimmer = trimmer;
            _signatures = signatures;
            _outputFile = outputFile;
        }
        
        @Override
        public void run() {
            
            try (PrintWriter out = FileSystem.openPrintWriter(_outputFile)) {
                SignatureBlocks sig;
                while ((sig = _signatures.next()) != null) {
                    HashIDSet blocks = new HashIDSet();
                    for (int columnId : _eqIndex.get(sig.id()).columns()) {
                        CentristTrimmer trimmer = _trimmer.get(columnId);
                        blocks.add(trimmer.trimmedBlocks(sig));
                    }
                    int sigSize = 0;
                    int usedBlocksSize = 0;
                    for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
                        int blockLen = sig.get(iBlock).length;
                        sigSize += blockLen;
                        if (blocks.contains(iBlock)) {
                            usedBlocksSize += blockLen;
                        }
                    }
                    out.println(
                            String.format(
                                    "%d\t%d\t%d\t%d\t%d",
                                    sig.id(),
                                    sig.size(),
                                    blocks.length(),
                                    sigSize,
                                    usedBlocksSize
                            )
                    );
                }
            } catch (java.io.IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
    
    public void run(
            EQIndex eqIndex,
            ConcurrentSignatureBlocksStream signatures,
            int threads,
            File outputDir
    ) {
        
        FileSystem.createFolder(outputDir);
        
        PrecisionScore scoreFunc = new PrecisionScore(eqIndex);
        
        HashMap<Integer, CentristTrimmer> trimmer = new HashMap<>();
        for (Column column : eqIndex.columns()) {
            trimmer.put(column.id(), new CentristTrimmer(column, scoreFunc));
        }
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            String filename = "centrist-blocks-usage." + iThread + ".tsv.gz";
            File outputFile = FileSystem.joinPath(outputDir, filename);
            OverlapComputer thread;
            thread = new OverlapComputer(eqIndex, trimmer, signatures, outputFile);
            es.execute(thread);
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <signatures-directory>\n" +
            "  <threads>\n" +
            "  <output-directory>";
    
    private final static Logger LOGGER = Logger
            .getLogger(CentristBlockUsageWriter.class.getName());
    
    public static void main(String[] args) {

        System.out.println("Centrist Block Usage Writer - Version (" + Constants.VERSION + ")\n");
        
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File signatureDir = new File(args[1]);
        int threads = Integer.parseInt(args[2]);
        File outputDir = new File(args[3]);
        
        EQIndex eqIndex = null;
        try {
            eqIndex = new EQIndex(eqFile);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "READ EQs", ex);
            System.exit(-1);
        }
        
        ConcurrentSignatureBlocksStream signatures = null;
        try {
            signatures = new ConcurrentSignatureBlocksStream(signatureDir);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "READ SIGNATURES", ex);
            System.exit(-1);
        }
        
        try {
            new CentristBlockUsageWriter()
                    .run(eqIndex, signatures, threads, outputDir);
        } catch (java.lang.RuntimeException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
