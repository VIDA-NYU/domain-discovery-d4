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
import org.opendata.core.object.IdentifiableObjectImpl;
import org.opendata.core.set.HashIDSet;

/**
 * List of blocks for a context signature. Contains the node identifier and the
 * similarity of the first entry in the context signature.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class SignatureBlocks extends IdentifiableObjectImpl {
    
    private final BigDecimal _maxSim;
    private final int _size;
    
    public SignatureBlocks(int id, BigDecimal maxSim, int size) {
        
        super(id);
        
        _maxSim = maxSim;
        _size = size;
    }
    
    public abstract int[] get(int index);
    
    public boolean isEmpty() {
        
        return (_size == 0);
    }
    
    public BigDecimal maxSim() {
        
        return _maxSim;
    }
    
    public HashIDSet nodes() {
    	
    	HashIDSet nodes = new HashIDSet();
        for (int iBlock = 0; iBlock < _size; iBlock++) {
        	for (int nodeId : this.get(iBlock)) {
        		nodes.add(nodeId);
        	}
        }
        return nodes;
    }
    
    public int nodeCount() {
    	
        int nodeCount = 0;
        for (int iBlock = 0; iBlock < _size; iBlock++) {
            nodeCount += this.get(iBlock).length;
        }
        return nodeCount;
    }
    
    public int size() {
        
        return _size;
    }
}
