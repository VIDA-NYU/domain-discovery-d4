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
package org.opendata.core.util.count;

import org.opendata.core.object.IdentifiableObjectImpl;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IdentifiableCount extends IdentifiableObjectImpl implements Comparable<IdentifiableCount> {
    
    private final int _count;
    
    public IdentifiableCount(int id, int count) {
        
        super(id);
        
        _count = count;
    }
    
    public IdentifiableCount(String[] valuePair) {
	
	this(Integer.parseInt(valuePair[0]), Integer.parseInt(valuePair[1]));
    }
    
    public IdentifiableCount(String pairString) {
	
	this(pairString.split(":"));
    }
    
    public IdentifiableCount add(int value) {
        
        return new IdentifiableCount(this.id(), _count + value);
    }

    @Override
    public int compareTo(IdentifiableCount c) {

        return Integer.compare(this.id(), c.id());
    }
    
    public int count() {
        
        return _count;
    }
    
    public String toPairString() {
	
	return this.id() + ":" + _count;
    }
}
