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
    
    /**
     * Get a block from the signature.
     * 
     * @param index
     * @return 
     */
    public abstract int[] get(int index);
    
    /**
     * Test if the signature is empty (has no blocks).
     * 
     * @return 
     */
    public boolean isEmpty() {
        
        return (_size == 0);
    }
    
    /**
     * Similarity of the first entry in the signature. This is the similarity
     * of the most similar term for the equivalence class that is represented
     * by this signature.
     * 
     * @return 
     */
    public BigDecimal maxSim() {
        
        return _maxSim;
    }
    
    /**
     * Number of blocks in the signature.
     * 
     * @return 
     */
    public int size() {
        
        return _size;
    }
}
