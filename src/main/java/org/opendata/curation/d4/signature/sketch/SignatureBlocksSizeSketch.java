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
package org.opendata.curation.d4.signature.sketch;

import java.util.ArrayList;
import java.util.List;
import org.opendata.curation.d4.signature.RobustSignature;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.RobustSignatureImpl;

/**
 * Consumer that prunes individual blocks in a signature. The size sketch
 * consumer ensures that no block in a signature contains more nodes than a
 * given threshold.
 * 
 * In the default implementation only the first n elements in each block are
 * kept.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksSizeSketch implements RobustSignatureConsumer {

    private final RobustSignatureConsumer _consumer;
    public final int _n;
    
    public SignatureBlocksSizeSketch(int n, RobustSignatureConsumer consumer) {
        
        _n = n;
        _consumer = consumer;
        
    }
    
    @Override
    public void close() {

        _consumer.close();
    }

    @Override
    public void consume(RobustSignature sig) {

        List<int[]> blocks = new ArrayList<>();
        for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
            int[] block = sig.get(iBlock);
            if (block.length > _n) {
                int[] trimmedBlock = new int[_n];
                System.arraycopy(block, 0, trimmedBlock, 0, _n);
                block = trimmedBlock;
            }
            blocks.add(block);
        }
        _consumer.consume(new RobustSignatureImpl(sig.id(), blocks));
    }

    @Override
    public void open() {

        _consumer.open();
    }
}
