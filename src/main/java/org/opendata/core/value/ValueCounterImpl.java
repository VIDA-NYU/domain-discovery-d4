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
package org.opendata.core.value;

/**
 * Default implementation of simple value counter.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ValueCounterImpl implements ValueCounter {
    
    private int _count;
    private final String _text;
    
    /**
     * Initialize text value and value counter.
     * 
     * @param text
     * @param count 
     */
    public ValueCounterImpl(String text, int count) {
	
	_text = text;
	_count = count;
    }
    
    /**
     * Initialize text value. The counter is initialized with 0 as default.
     * 
     * @param text 
     */
    public ValueCounterImpl(String text) {
	
        this(text, 0);
    }

    @Override
    public int compareTo(ValueCounter value) {

        return Integer.compare(_count, value.getCount());
    }

    @Override
    public int getCount() {
	
        return _count;
    }

    @Override
    public String getText() {
	
        return _text;
    }
    
    @Override
    public int incCount() {
	
        return ++_count;
    }
    
    @Override
    public int incCount(int increment) {
	
        return (_count += increment);
    }
    
    @Override
    public boolean isEmpty() {
        
        return _text.trim().equals("");
    }
}