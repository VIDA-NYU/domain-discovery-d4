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

/**
 * Basic (un-synchronized) implementation for the counter class.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimpleCounter implements Counter {

    private int _value;
    
    public SimpleCounter(int value) {
        
        _value = value;
    }
    
    public SimpleCounter() {
        
        this(0);
    }
    
    @Override
    public int compareTo(Counter c) {

        return Integer.compare(_value, c.value());
    }

    @Override
    public int inc() {

        return _value++;
    }

    @Override
    public int inc(int value) {

        return _value += value;
    }

    @Override
    public int value() {

        return _value;
    }    
}
