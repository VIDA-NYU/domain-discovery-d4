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
 * Simple counter object. Main use is to keep track of a 'global' integer
 * throughout recursive methods calls.
 * 
 * @author Heiko Mueller
 */
public class Counter implements Comparable<Counter> {
   
    private int _value;
    
    public Counter(int value) {
	
	_value = value;
    }
    
    public Counter() {
	
	this(0);
    }

    @Override
    public int compareTo(Counter cntr) {

        return Integer.compare(_value, cntr.value());
    }
    
    public synchronized int inc() {
	
        return ++_value;
    }
    
    public synchronized int inc(int value) {
	
	return _value += value;
    }
    
    public synchronized void setValue(int value) {
        
        _value = value;
    }
    
    public synchronized int value() {
	
	return _value;
    }
}
