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

import java.io.BufferedReader;
import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.opendata.core.io.FileSetReader;
import org.opendata.core.io.FileSystem;

/**
 * Reader for a signature blocks file. Generates a stream of signature blocks
 * for a given consumer.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksReader extends FileSetReader implements SignatureBlocksStream {
   
    public SignatureBlocksReader(File file, boolean verbose) {
        
        super(file, verbose);
    }

    public SignatureBlocksReader(File file) {
        
        this(file, false);
    }
    
    public SignatureBlocksReader(List<File> files, File directory) {
        
        super(files, false);
    }
    
    public SignatureBlocksIndex read() throws java.io.IOException {
        
        SignatureBlocksIndex buffer = new SignatureBlocksIndex();
        this.stream(buffer);
        return buffer;
    }
    
    @Override
    public void stream(SignatureBlocksConsumer consumer) {

        consumer.open();

        for (File file : this) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    int eqId = Integer.parseInt(tokens[0]);
                    BigDecimal sim = new BigDecimal(tokens[1]);
                    List<SignatureBlock> blocks = new ArrayList<>();
                    for (int iToken = 2; iToken < tokens.length; iToken++) {
                        String[] blockTokens = tokens[iToken].split(",");
                        int termCount = Integer.parseInt(blockTokens[0]);
                        Integer[] blockElements = new Integer[blockTokens.length - 1];
                        for (int iBT = 1; iBT < blockTokens.length; iBT++) {
                            String token = blockTokens[iBT];
                            int pos = token.indexOf(":");
                            blockElements[iBT - 1] = Integer.parseInt(token.substring(0, pos));
                        }
                        blocks.add(new SignatureBlockImpl(blockElements, termCount));
                    }
                    consumer.consume(eqId, sim, blocks);
                }
            } catch (java.io.IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        consumer.close();
    }
}
