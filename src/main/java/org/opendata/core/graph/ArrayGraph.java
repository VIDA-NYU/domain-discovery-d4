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
import org.opendata.core.set.IDSet;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ArrayGraph extends AdjacencyGraph {

    private final boolean[][] _edges;
    private final int[] _edgeCount;
    private final HashMap<Integer, Integer> _inverseMapping;
    private final HashMap<Integer, Integer> _mapping;
    private final boolean _reversed;
    private final int _size;
    
    public ArrayGraph(IDSet nodes) {
        
        super(nodes);
        
        _size = nodes.length();
        _edges = new boolean[_size][_size];
        _edgeCount = new int[_size];
        _reversed = false;
        
        _mapping = new HashMap<>();
        _inverseMapping = new HashMap<>();
        int index = 0;
        for (int nodeId : nodes.toArray()) {
            _mapping.put(nodeId, index);
            _inverseMapping.put(index, nodeId);
            index++;
        }
    }
    
    public ArrayGraph(
            IDSet nodes,
            boolean[][] edges,
            HashMap<Integer, Integer> mapping,
            HashMap<Integer, Integer> inverseMapping,
            boolean reversed
    ) {
        super(nodes);
        
        _edges = edges;
        _mapping = mapping;
        _inverseMapping = inverseMapping;
        _reversed = reversed;
        
        _size = nodes.length();

        _edgeCount = new int[_size];
        for (int iPos = 0; iPos < _size; iPos++) {
            _edgeCount[iPos] = 0;
            for (int iEdge = 0; iEdge < _size; iEdge++) {
                if (_edges[iEdge][iPos]) {
                    _edgeCount[iPos]++;
                }
            }
        }
    }
    
    public void add(int nodeId, int[] edges) {
        
        int index = _mapping.get(nodeId);
        for (int target : edges) {
            _edges[index][_mapping.get(target)] = true;
        }
        _edgeCount[index] = edges.length;
    }
    
    @Override
    public Iterable<Integer> adjacent(int nodeId) {

        int index = _mapping.get(nodeId);
        return new ArrayIterable(
                _edges,
                _edgeCount[index],
                index,
                _inverseMapping,
                _reversed
        );
    }

    @Override
    public AdjacencyGraph reverse() {

        return new ArrayGraph(
                this.nodes(),
                _edges,
                _mapping,
                _inverseMapping,
                !_reversed
        );
    }
}
