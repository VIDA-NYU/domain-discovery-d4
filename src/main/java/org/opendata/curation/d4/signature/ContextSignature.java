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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.opendata.core.object.IdentifiableObject;
import org.opendata.core.sort.DoubleValueDescSort;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ContextSignature implements Comparable<ContextSignature>, IdentifiableObject {
    
    private final ContextSignatureValue[] _elements;
    private final int _id;
    
    public ContextSignature(int id, List<ContextSignatureValue> elements) {
        
	_id = id;
        _elements = new ContextSignatureValue[elements.size()];
        for (int iElement = 0; iElement < elements.size(); iElement++) {
            _elements[iElement] = elements.get(iElement);
        }
        Arrays.sort(_elements);
    }

    public ContextSignature(int id) {

	this(id, new ArrayList<ContextSignatureValue>());
    }
    
    @Override
    public int compareTo(ContextSignature vec) {

        return Integer.compare(this.size(), vec.size());
    }
    
    public List<ContextSignatureValue> elements() {
    
        return Arrays.asList(_elements);
    }

    @Override
    public int id() {
	
	return _id;
    }
    
    public boolean isEmpty() {
        
        return (_elements.length == 0);
    }
    
    public List<ContextSignatureValue> rankedElements() {
        
        List<ContextSignatureValue> elements = this.elements();
        Collections.sort(elements, new DoubleValueDescSort<>());
        return elements;
    }
    
    public int size() {
        
        return _elements.length;
    }
}
