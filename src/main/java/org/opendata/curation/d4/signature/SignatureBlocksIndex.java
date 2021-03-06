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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * In-memory index for signature blocks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksIndex implements SignatureBlocksStream, SignatureBlocksConsumer {

    private class IndexElement {
        
        private final List<SignatureBlock> _blocks;
        private final BigDecimal _sim;
        
        public IndexElement(BigDecimal sim, List<SignatureBlock> blocks) {
            
            _sim = sim;
            _blocks = blocks;
        }
        
        public List<SignatureBlock> blocks() {
            
            return _blocks;
        }
        
        public BigDecimal sim() {
            
            return _sim;
        }
    }
    
    private final HashMap<Integer, IndexElement> _elements = new HashMap<>();
    
    @Override
    public void close() {

    }

    @Override
    public void consume(int nodeId, BigDecimal sim, List<SignatureBlock> blocks) {

        _elements.put(nodeId, new IndexElement(sim, blocks));
    }

    public List<SignatureBlock> get(int nodeId) {
    
        if (_elements.containsKey(nodeId)) {
            return _elements.get(nodeId).blocks();
        } else {
            return new ArrayList<>();
        }
    }
    
    public Set<Integer> keys() {
        
        return _elements.keySet();
    }
    
    @Override
    public void open() {

    }

    @Override
    public void stream(SignatureBlocksConsumer consumer) {
        
        consumer.open();
        
        for (Integer nodeId : _elements.keySet()) {
            IndexElement el = _elements.get(nodeId);
            consumer.consume(nodeId, el.sim(), el.blocks());
        }
        consumer.close();
    }
}
