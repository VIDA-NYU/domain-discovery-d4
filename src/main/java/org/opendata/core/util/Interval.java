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
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Interval {
    
    private final int _end;
    private final int _start;
    
    public Interval(int start, int end) {
    
        if (start > end) {
            throw new IllegalArgumentException("Invalid interval: [" + start + "-" + end + "]");
        }
        _start = start;
        _end = end;
    }
    
    public Interval(int start) {
        
        this(start, start);
    }
    
    public boolean contains(Interval interval) {
        
        return (_start <= interval.start() && _end >= interval.end());
    }
    
    public int end() {
        
        return _end;
    }
    
    public int start() {
        
        return _start;
    }
    
    @Override
    public String toString() {
        
        return "[" + _start + "-" + _end + "]";
    }
}
