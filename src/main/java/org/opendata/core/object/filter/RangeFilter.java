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
package org.opendata.core.object.filter;

/**
 * Filter that accepts identifiers within a given interval.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class RangeFilter implements ObjectFilter<Integer> {

    private final int _end;
    private final int _start;
    
    public RangeFilter(int start, int end) {
    
        _start = start;
        _end = end;
    }
    
    @Override
    public boolean contains(Integer nodeId) {
        
        return ((_start <= nodeId) && (nodeId < _end));
    }
}
