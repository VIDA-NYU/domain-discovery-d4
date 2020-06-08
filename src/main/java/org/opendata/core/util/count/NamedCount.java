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

/**
 * Counter that is associated with a (unique) name.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class NamedCount implements Comparable<NamedCount> {
    
    private int _count;
    private final String _name;
    
    public NamedCount(String name, int count) {
        
        _name = name;
        _count = count;
    }
    
    public NamedCount(String name) {
	
    	this(name, 0);
    }

    @Override
    public int compareTo(NamedCount c) {

        return Integer.compare(this.count(), c.count());
    }
    
    public int count() {
        
        return _count;
    }

    public int inc(int value) {
    	
    	_count += value;
    	return _count;
    }
    
    public int inc() {
    	
    	return this.inc(1);
    }
    
    public String name() {
    	
    	return _name;
    }
}
