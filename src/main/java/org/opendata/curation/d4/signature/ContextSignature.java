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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.opendata.core.object.IdentifiableObject;
import org.opendata.core.prune.CandidateSetFinder;

/**
 * Vector of similarity values for an equivalence class. The context
 * signature maintains similarity values for all other equivalence
 * classes that the equivalence class occurs with.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ContextSignature implements IdentifiableObject {
    
    private final SignatureValue[] _elements;
    private final int _id;
    
    public ContextSignature(int id, List<SignatureValue> elements) {
        
    	_id = id;
        _elements = new SignatureValue[elements.size()];
        for (int iElement = 0; iElement < elements.size(); iElement++) {
            _elements[iElement] = elements.get(iElement);
        }
        Arrays.sort(_elements);
    }

    public ContextSignature(int id) {

    	this(id, new ArrayList<SignatureValue>());
    }
    
    public List<SignatureValue> elements() {
    
        return Arrays.asList(_elements);
    }

    @Override
    public int id() {
	
    	return _id;
    }
    
    public boolean isEmpty() {
        
        return (_elements.length == 0);
    }
    
    public List<SignatureValue> rankedElements() {
        
        List<SignatureValue> elements = this.elements();
        Collections.sort(elements, Collections.reverseOrder());
        return elements;
    }
    
    public int size() {
        
        return _elements.length;
    }
    
    public SignatureBlocks toSignatureBlocks(CandidateSetFinder<SignatureValue> candidateFinder) {
    	
        List<SignatureValue> sig = this.rankedElements();
        // No output if the context signature is empty
        if (sig.isEmpty()) {
            return new SignatureBlocksImpl(_id, BigDecimal.ZERO, new int[0][0]);
        }
        int start = 0;
        final int end = sig.size();
        ArrayList<int[]> blocks = new ArrayList<>();
        while (start < end) {
            int pruneIndex = candidateFinder.getPruneIndex(sig, start);
            if (pruneIndex <= start) {
                break;
            }
            int[] block = new int[pruneIndex - start];
            for (int iEl = start; iEl < pruneIndex; iEl++) {
                block[iEl - start] = sig.get(iEl).id();
            }
            Arrays.sort(block);
            blocks.add(block);
            start = pruneIndex;
        }
        return new SignatureBlocksImpl(
                _id,
                sig.get(0).value(),
                blocks
        );
   }
}
