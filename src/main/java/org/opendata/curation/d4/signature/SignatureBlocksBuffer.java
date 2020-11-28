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
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.util.MemUsagePrinter;

/**
 * Memory buffer for signature blocks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksBuffer implements Iterable<SignatureBlocks>, SignatureBlocksConsumer, SignatureBlocksStream {

    private final List<SignatureBlocks> _signatures = new ArrayList<>();
    
    @Override
    public void close() {

    }

    @Override
    public void consume(SignatureBlocks sig) {

        _signatures.add(sig);
    }

    public SignatureBlocks get(int index) {
        
        return _signatures.get(index);
    }

    @Override
    public boolean isDone() {
        
        return false;
    }
    
    @Override
    public Iterator<SignatureBlocks> iterator() {

        return _signatures.iterator();
    }

    @Override
    public void open() {

        _signatures.clear();
    }
    
    public int size() {
        
        return _signatures.size();
    }

    @Override
    public void stream(SignatureBlocksConsumer consumer) {

        consumer.open();
        
        for (SignatureBlocks sig : _signatures) {
            consumer.consume(sig);
        }
        
        consumer.close();
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <signatures-file-or-dir>";
    
    private static final Logger LOGGER = Logger
            .getLogger(SignatureBlocksBuffer.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File signaturesFileOrDir = new File(args[0]);
        
        SignatureBlocksBuffer buffer = new SignatureBlocksBuffer();
        
        Date start = new Date();
        try {
            new SignatureBlocksReader(signaturesFileOrDir, true).stream(buffer);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
        Date end = new Date();
        
        long execTime = end.getTime() - start.getTime();
        
        new MemUsagePrinter().print();
        
        System.out.println("\n\nREAD " + buffer.size() + " SIGNATURES IN " + execTime + " ms");
    }
}
