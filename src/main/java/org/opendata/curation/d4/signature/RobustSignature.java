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

import org.opendata.core.object.IdentifiableObjectImpl;
import org.opendata.core.set.SortedIDList;

/**
 * List of blocks for a robust context signature. The robust signature contains
 * on;y the node identifier for each block but no similarity statistics.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class RobustSignature extends IdentifiableObjectImpl {
    
    private final int _size;
    
    public RobustSignature(int id, int size) {
        
        super(id);
        
        _size = size;
    }
    
    /**
     * Get a block from the signature.
     * 
     * @param index
     * @return 
     */
    public abstract SortedIDList get(int index);
    
    /**
     * Test if the signature is empty (has no blocks).
     * 
     * @return 
     */
    public boolean isEmpty() {
        
        return (_size == 0);
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
