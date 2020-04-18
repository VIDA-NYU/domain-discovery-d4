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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSetReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.util.ArrayHelper;

/**
 * Reader for a signature blocks file. Generates a stream of signature blocks
 * for a given consumer.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksReader extends FileSetReader implements SignatureBlocksStream {
   
    private final static Logger LOGGER = Logger
            .getLogger(SignatureBlocksReader.class.getName());
    
    public SignatureBlocksReader(File file, boolean verbose) {
        
        super(file, verbose);
    }

    public SignatureBlocksReader(File file) {
        
        this(file, false);
    }
    
    public SignatureBlocksReader(List<File> files) {
        
        super(files, true);
    }
    
    public SignatureBlocksIndex read() throws java.io.IOException {
        
        SignatureBlocksIndex buffer = new SignatureBlocksIndex();
        this.stream(buffer);
        return buffer;
    }
    
    @Override
    public void stream(SignatureBlocksConsumer consumer) throws java.io.IOException {

        consumer.open();

        for (File file : this) {
            System.out.println("READING " + file.getName());
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    int[][] blocks = new int[tokens.length - 2][];
                    for (int iToken = 2; iToken < tokens.length; iToken++) {
                        blocks[iToken - 2] = ArrayHelper.arrayFromString(tokens[iToken]);
                    }
                    SignatureBlocks sig = new SignatureBlocksImpl(
                            Integer.parseInt(tokens[0]),
                            new BigDecimal(tokens[1]),
                            blocks
                    );
                    consumer.consume(sig);
                }
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, file.getName(), ex);
            }
        }

        consumer.close();
    }
}
