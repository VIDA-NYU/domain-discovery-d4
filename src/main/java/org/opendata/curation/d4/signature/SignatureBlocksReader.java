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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.opendata.core.io.FileSetReader;
import org.opendata.core.io.FileSystem;

/**
 * Reader for a signature blocks file. Generates a stream of signature blocks
 * for a given consumer.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksReader extends FileSetReader implements RobustSignatureStream {
   
    private final File _file;
    
    public SignatureBlocksReader(File file, boolean verbose) {
        
        super(file, verbose);
        
        _file = file;
    }

    public SignatureBlocksReader(File file) {
        
        this(file, false);
    }
    
    public SignatureBlocksReader(List<File> files, File directory) {
        
        super(files, false);
        
        _file = directory;
    }
    
    private SignatureBlock getBlock(String text) {
        
        String[] tokens = text.substring(0, text.indexOf(":")).split("-");
        return new SignatureBlock(
                this.getBlockNodes(text),
                Double.parseDouble(tokens[0]),
                Double.parseDouble(tokens[1])
        );
    }
    
    private int[] getBlockNodes(String text) {
        
        String[] tokens = text.substring(text.indexOf(":") + 1).split(",");
        int[] nodes = new int[tokens.length];
        for (int iToken = 0; iToken < tokens.length; iToken++) {
            nodes[iToken] = Integer.parseInt(tokens[iToken]);
        }
        Arrays.sort(nodes);
        return nodes;
    }
    
    public RobustSignatureIndex read() throws java.io.IOException {
        
        RobustSignatureIndex buffer = new RobustSignatureIndex(this.source());
        this.stream(buffer);
        return buffer;
    }
    
    @Override
    public void stream(RobustSignatureConsumer consumer) throws java.io.IOException {

        consumer.open();

        for (File file : this) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    int[][] blocks = new int[tokens.length - 1][];
                    for (int iToken = 1; iToken < tokens.length; iToken++) {
                        blocks[iToken - 1] = this.getBlockNodes(tokens[iToken]);
                    }
                    RobustSignature sig = new RobustSignatureImpl(
                            Integer.parseInt(tokens[0]),
                            blocks
                    );
                    consumer.consume(sig);
                }
            }
        }

        consumer.close();
    }

    public void stream(SignatureBlocksConsumer consumer) throws java.io.IOException {

        consumer.open();

        for (File file : this) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    ArrayList<SignatureBlock> blocks = new ArrayList<>();
                    for (int iToken = 1; iToken < tokens.length; iToken++) {
                        blocks.add(this.getBlock(tokens[iToken]));
                    }
                    consumer.consume(Integer.parseInt(tokens[0]), blocks);
                }
            }
        }

        consumer.close();
    }

    @Override
    public String source() {

        return _file.getName();
    }
}
