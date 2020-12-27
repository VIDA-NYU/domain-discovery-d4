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
package org.opendata.curation.d4.signature.trim;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.opendata.curation.d4.signature.ContextSignatureBlock;
import org.opendata.curation.d4.signature.ContextSignatureBlocksConsumer;

/**
 * Base class for generating robust signatures for individual equivalence
 * classes. Robust signatures are context signatures that have been divided
 * into signature blocks. A robust signature will only maintain those blocks
 * that are not classified as noisy blocks. The definition of what constitutes
 * a noisy block is implementation dependent.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class SignatureRobustifier implements ContextSignatureBlocksConsumer {
    
    private final ContextSignatureBlocksConsumer _consumer;
        
    /**
     * Initialize the consumer for the robust signature blocks.
     * 
     * @param consumer 
     */
    public SignatureRobustifier(ContextSignatureBlocksConsumer consumer) {

        _consumer = consumer;
    }
    
    @Override
    public void close() {

        _consumer.close();
    }
    
    @Override
    public void open() {

        _consumer.open();
    }
    
    /**
     * Push robust signature to associated consumer. Passes only a prefix of
     * the block list to the underlying consumer.
     * 
     * @param nodeId
     * @param sim
     * @param blocks 
     * @param end 
     */
    public void push(int nodeId, BigDecimal sim, List<ContextSignatureBlock> blocks, int end) {

        ArrayList<ContextSignatureBlock> prunedBlocks = new ArrayList<>();
        for (int iBlock = 0; iBlock < end; iBlock++) {
            prunedBlocks.add(blocks.get(iBlock));
        }
        
        _consumer.consume(nodeId, sim, prunedBlocks);
    }
}
