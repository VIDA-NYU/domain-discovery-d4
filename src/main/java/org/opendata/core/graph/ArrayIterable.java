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
package org.opendata.core.graph;

import java.util.HashMap;
import java.util.Iterator;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ArrayIterable implements Iterable<Integer>, Iterator<Integer> {

    private final boolean[][] _edges;
    private final int _edgeCount;
    private final int _index;
    private final HashMap<Integer, Integer> _inverseMapping;
    private int _readCount;
    private int _readIndex;
    private final boolean _reversed;
    
    public ArrayIterable(boolean[][] edges, int edgeCount, int index, HashMap<Integer, Integer> inverseMapping, boolean reversed) {
        
        _edges = edges;
        _edgeCount = edgeCount;
        _index = index;
        _inverseMapping = inverseMapping;
        _reversed = reversed;
        
        _readCount = 0;
        _readIndex = 0;
    }
    
    @Override
    public Iterator<Integer> iterator() {
        
        return this;
    }

    @Override
    public boolean hasNext() {

        return (_readCount < _edgeCount);
    }

    @Override
    public Integer next() {

        int nodeId = -1;
        
        if (_reversed) {
            while (true) {
                if (_edges[_readIndex][_index]) {
                    nodeId = _inverseMapping.get(_readIndex++);
                    break;
                } else {
                    _readIndex++;
                }
            }            
        } else {
            while (true) {
                if (_edges[_index][_readIndex]) {
                    nodeId = _inverseMapping.get(_readIndex++);
                    break;
                } else {
                    _readIndex++;
                }
            }            
        }
        
        _readCount++;
        return nodeId;
    }
}
