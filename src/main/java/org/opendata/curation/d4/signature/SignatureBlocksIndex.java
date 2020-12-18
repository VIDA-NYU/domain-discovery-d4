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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * In-memory index for signature blocks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksIndex implements RobustSignatureStream, SignatureBlocksConsumer {

    private final HashMap<Integer, List<SignatureBlock>> _elements = new HashMap<>();
    
    @Override
    public void close() {

    }

    @Override
    public void consume(int nodeId, List<SignatureBlock> blocks) {

        _elements.put(nodeId, blocks);
    }

    public List<SignatureBlock> get(int nodeId) {
    
        if (_elements.containsKey(nodeId)) {
            return _elements.get(nodeId);
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
    public void stream(RobustSignatureConsumer consumer) {
        
        consumer.open();
        
        for (Integer nodeId : _elements.keySet()) {
            RobustSignature sig;
            sig = new RobustSignatureBlocks(nodeId, _elements.get(nodeId));
            consumer.consume(sig);
        }
        consumer.close();
    }

    @Override
    public String source() {

        return SignatureBlocksIndex.class.getCanonicalName();
    }

    @Override
    public String target() {

        return SignatureBlocksIndex.class.getCanonicalName();
    }
}
