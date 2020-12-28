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

import org.opendata.core.set.SortedObjectSet;

/**
 * Block of elements in a context signature. Context signature blocks are
 * generated as part of the signature robustification process.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ContextSignatureBlock extends SortedObjectSet<ContextSignatureValue> implements SignatureBlock {
    
    private final int _termCount;
    
    public ContextSignatureBlock(ContextSignatureValue[] elements, int termCount) {
        
        super(elements);
        
        _termCount = termCount;
    }

    @Override
    public Integer elementAt(int index) {

        return this.objectAt(index).id();
    }

    @Override
    public int elementCount() {

        return this.objectCount();
    }
    
    @Override
    public int termCount() {
        
        return _termCount;
    }
}
