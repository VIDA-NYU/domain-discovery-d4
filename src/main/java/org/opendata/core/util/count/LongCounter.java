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
 * Simple counter object. Uses long as counter.
 * 
 * @author Heiko Mueller
 */
public class LongCounter implements Comparable<LongCounter> {
   
    private long _value;
    
    public LongCounter(long value) {
	
	_value = value;
    }
    
    public LongCounter() {
	
	this(0);
    }

    @Override
    public int compareTo(LongCounter cntr) {

        return Long.compare(_value, cntr.value());
    }
    
    public synchronized long inc() {
	
	return ++_value;
    }
    
    public synchronized long inc(long value) {
	
	return _value += value;
    }
    
    public synchronized void setValue(long value) {
        
        _value = value;
    }
    
    public synchronized long value() {
	
	return _value;
    }
}
