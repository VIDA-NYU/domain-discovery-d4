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

import org.opendata.core.object.IdentifiableInteger;

/**
 * Identifiable counter object.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IdentifiableCount extends IdentifiableInteger implements Counter {
    
    private int _count;
    
    public IdentifiableCount(int id, int count) {
        
        super(id);
        
        _count = count;
    }
    
    public IdentifiableCount(String[] splitString) {
	
	this(Integer.parseInt(splitString[0]), Integer.parseInt(splitString[1]));
    }
    
    public IdentifiableCount(String fromString) {
	
	this(fromString.split(":"));
    }

    @Override
    public int compareTo(Counter c) {

        return Integer.compare(_count, c.value());
    }
    
    @Override
    public int inc(int value) {

        _count += value;
        return _count;
    }
    
    @Override
    public int inc() {
        
        return this.inc(1);
    }

    @Override
    public String toString() {
	
	return this.id() + ":" + _count;
    }
    
    @Override
    public int value() {
        
        return _count;
    }
}
