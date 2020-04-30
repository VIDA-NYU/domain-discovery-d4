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
package org.opendata.curation.d4.export;

import java.io.File;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.curation.d4.signature.SignatureBlocksStream;

/**
 * Write maximum similarity information from robust signature blocks for each
 * equivalence class.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQSigSimWriter {
    
    private class SigSimWriter implements SignatureBlocksConsumer {

        private final PrintWriter _out;
        
        public SigSimWriter(PrintWriter out) {
            
            _out = out;
        }
        
        @Override
        public void close() {

        }

        @Override
        public void consume(SignatureBlocks sig) {

            _out.println(sig.id() + "\t" + new FormatedBigDecimal(sig.maxSim()));
        }

        @Override
        public void open() {

        }
    }
    
    public void run(SignatureBlocksStream signatures, PrintWriter out) throws java.io.IOException {
        
        signatures.stream(new SigSimWriter(out));
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <signature-file>\n" +
            "  <output-file";
    
    private final static Logger LOGGER = Logger
            .getLogger(EQSigSimWriter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File signatureFile = new File(args[0]);
        File outputFile = new File(args[1]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new EQSigSimWriter()
                    .run(new SignatureBlocksReader(signatureFile), out);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
