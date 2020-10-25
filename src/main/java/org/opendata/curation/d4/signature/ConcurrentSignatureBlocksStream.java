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
import java.util.LinkedList;
import org.opendata.core.io.FileSetReader;
import org.opendata.core.io.FileSystem;

/**
 * Reader for a signature blocks file. Generates a stream of signature blocks
 * for a given consumer.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ConcurrentSignatureBlocksStream extends FileSetReader {
   
    private final LinkedList<File> _files;
    private BufferedReader _in = null;
    
    public ConcurrentSignatureBlocksStream(File file, boolean verbose) throws java.io.IOException {
        
        super(file, verbose);
        
        _files = new LinkedList<>();
        for (File inputFile : this) {
            _files.add(inputFile);
        }
        
        if (!_files.isEmpty()) {
            _in = FileSystem.openReader(_files.pop());
        }
    }

    public ConcurrentSignatureBlocksStream(File file) throws java.io.IOException {
        
        this(file, false);
    }
    
    public synchronized SignatureBlocks next() throws java.io.IOException {
        
        while (_in != null) {
            String line = _in.readLine();
            if (line != null) {
                String[] tokens = line.split("\t");
                int[][] blocks = new int[tokens.length - 2][];
                for (int iToken = 2; iToken < tokens.length; iToken++) {
                    blocks[iToken - 2] = SignatureBlocksReader.
                            getBlockNodes(tokens[iToken]);
                }
                return new SignatureBlocksImpl(
                        Integer.parseInt(tokens[0]),
                        new BigDecimal(tokens[1]),
                        blocks
                );
            } else {
                _in.close();
                _in = null;
                if (!_files.isEmpty()) {
                    _in = FileSystem.openReader(_files.pop());
                }
            }
        }
        
        return null;
    }
}
