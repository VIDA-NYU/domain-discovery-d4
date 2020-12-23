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
package org.opendata.core.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.ImmutableIDSet;

/**
 * Set of identifiable counter objects.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IdentifiableCounterSet implements Iterable<IdentifiableCount> {
   
    private final HashMap<Integer, Counter> _elements = new HashMap<>();
    
    public final int add(int id, int value) {
        
        _elements.put(id, new SimpleCounter(value));
        
        return value;
    }
    
    public final Counter get(int id) {
        
        if (!_elements.containsKey(id)) {
            _elements.put(id, new SimpleCounter());
        }
        return _elements.get(id);
    }
    
    public int getMaxId() {

	int maxId = 0;
	for (int id : _elements.keySet()) {
	    if (id > maxId) {
		maxId = id;
	    }
	}
	return maxId;
    }
    
    public int getMaxValue() {

	int maxCount = 0;
	for (Counter c : _elements.values()) {
	    if (c.value() > maxCount) {
		maxCount = c.value();
	    }
	}
	return maxCount;
    }
    
    public final int inc(int id, int value) {
        
        if (_elements.containsKey(id)) {
            return _elements.get(id).inc(value);
        } else {
            return this.add(id, value);
        }
    }
    
    public int inc(int id) {
        
        return this.inc(id, 1);
    }

    @Override
    public Iterator<IdentifiableCount> iterator() {

        return this.toList().iterator();
    }
    
    public final IDSet keys() {
        
        return new ImmutableIDSet(_elements.keySet());
    }
    
    public int size() {
	
	return _elements.size();
    }
    
    public List<IdentifiableCount> toList() {
        
        List<IdentifiableCount> result = new ArrayList<>();
        for (int id : _elements.keySet()) {
            result.add(new IdentifiableCount(id, _elements.get(id).value()));
        }
        return result;
    }
    
    public List<IdentifiableCount> toSortedList(boolean reverse) {
        
        List<IdentifiableCount> result = this.toList();
        Collections.sort(
                result,
                (IdentifiableCount c1, IdentifiableCount c2) -> 
                        Integer.compare(c1.value(), c2.value())
        );
        if (reverse) {
            Collections.reverse(result);
        }
        return result;
    }
}
