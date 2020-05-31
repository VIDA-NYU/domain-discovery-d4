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
package org.opendata.core.set;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.opendata.core.metric.JaccardIndex;
import org.opendata.core.util.StringHelper;

/**
 * Implements the basic methods of identifier sets.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class IDSetImpl extends ObjectSetImpl<Integer> implements IDSet {
    
    /**
     * Create a new instance of the IDSet from a given list of values.
     * 
     * @param values
     * @return 
     */
    public abstract IDSet create(Collection<Integer> values);
    
    @Override
    public IDSet difference(IDSet list) {

        HashSet<Integer> values = new HashSet<>();
        for (int id : this) {
            if (!list.contains(id)) {
                values.add(id);
            }
        }
        return this.create(values);
    }
    
    @Override
    public IDSet difference(int value) {

        HashSet<Integer> values = new HashSet<>();
        for (int id : this) {
            if (id != value) {
                values.add(id);
            }
        }
        return this.create(values);
    }

    @Override
    public BigDecimal ji(IDSet list) {
    
        return new JaccardIndex()
        		.sim(this.length(), list.length(), this.overlap(list));
    }
    
    @Override
    public int maxId() {

        int maxId = -1;
        for (int id : this) {
            if (id > maxId) {
                maxId = id;
            }
        }
        return maxId;
    }

    @Override
    public int minId() {

	// For an empty set the minimum is 0 by default
	if (this.isEmpty()) {
	    return 0;
	}
	
        int minId = Integer.MAX_VALUE;
        for (int id : this) {
            if (id < minId) {
		minId = id;
	    }
        }
        return minId;
    }

    @Override
    public IDSet intersect(IDSet list) {

	IDSet innerList;
	IDSet outerList;
	
        if (this.length() > list.length()) {
	    outerList = list;
	    innerList = this;
	} else {
	    outerList = this;
	    innerList = list;
	}
	
        HashSet<Integer> values = new HashSet<>();
        for (int id : outerList) {
            if (innerList.contains(id)) {
                values.add(id);
            }
        }
        
        return this.create(values);
    }

    
    @Override
    public boolean isTrueSubsetOf(IDSet list) {
	
	if (this.length() < list.length()) {
	    for (int nodeId : this) {
		if (!list.contains(nodeId)) {
		    return false;
		}
	    }
            return true;
	}
        return false;
    }
    
    @Override
    public boolean overlaps(IDSet set, int threshold) {
        
        if (threshold <= 0) {
            return true;
        }
        
        IDSet inner;
        IDSet outer;

        if (this.length() < threshold) {
            return false;
        } else if (set.length() < threshold) {
            return false;
        } else if (this.length() < set.length()) {
            inner = this;
            outer = set;
        } else {
            inner = set;
            outer = this;
        }

        int overlap = 0;
        for (int termId : outer) {
            if (inner.contains(termId)) {
                overlap++;
                if (overlap == threshold) {
                    return true;
                }
            }
        }

        return false;
    }
    
    @Override
    public IDSet sample(int size) {
    
        if (size >= this.length()) {
            return this;
        }
        
        Random rand = new Random();
        
        HashSet<Integer> result = new HashSet<>();
        
        List<Integer> valueList = this.toList();
        while (result.size() < size) {
            int val = valueList.get(rand.nextInt(this.length()));
            if (!result.contains(val)) {
                result.add(val);
            }
        }
        return this.create(result);
    }
    
    @Override
    public String toIntString() {
	
        List<Integer> values = this.toList();
        Collections.sort(values);
        return StringHelper.joinIntegers(values);
    }

    @Override
    public List<Integer> toSortedList() {
    
        List<Integer> result = this.toList();
        Collections.sort(result);
        return result;
    }
    
    @Override
    public String toString() {
    
        StringBuilder buf = new StringBuilder("[");
        int count = 0;
        for (int id : this) {
            if (count > 0) {
                buf.append(",");
            }
            buf.append(id);
            count++;
        }
        buf.append("]");
        return buf.toString();
    }    

    @Override
    public IDSet union(IDSet list) {
        
        HashSet<Integer> values = new HashSet<>(this.toList());
        for (int value : list) {
            values.add(value);
        }
        return this.create(values);
    }    
    
    @Override
    public IDSet union(int nodeId) {

        HashSet<Integer> values = new HashSet<>(this.toList());
        values.add(nodeId);
        return this.create(values);
    }
}
