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

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.opendata.core.set.IDSet;
import org.opendata.core.util.ArrayHelper;
import org.opendata.core.util.StringHelper;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureGraph extends AdjacencyGraph {

    private final boolean[][] _edges;
    private final HashMap<Integer, Integer> _nodeToIndexMap;
    private final List<Integer> _indexToNodeMap;
    private final boolean _reversed;
    
    public SignatureGraph(IDSet nodes) {
	
        super(nodes);

        _edges = new boolean[nodes.length()][nodes.length()];
        _reversed = false;

        _indexToNodeMap = nodes.toSortedList();
        _nodeToIndexMap = new HashMap<>();
        int index = 0;
        for (int nodeId : _indexToNodeMap) {
            _nodeToIndexMap.put(nodeId, index++);
        }
    }
    
    public SignatureGraph(IDSet nodes, boolean[][] edges, HashMap<Integer, Integer> nodeToIndexMap, List<Integer> indexToNodeMap) {
	
        super(nodes);

        _edges = edges;
        _reversed = true;
        _nodeToIndexMap = nodeToIndexMap;
        _indexToNodeMap = indexToNodeMap;
    }
    
    public SignatureGraph(IDSet nodes, BufferedReader in) throws java.io.IOException {
        
        super(nodes);
        
        // Read index to node map
        _indexToNodeMap = new ArrayList<>();
        for (int nodeId : ArrayHelper.arrayFromString(in.readLine())) {
            _indexToNodeMap.add(nodeId);
        }
        
        // Read node to index map next
        _nodeToIndexMap = new HashMap<>();
        for (int iNode = 0; iNode < _indexToNodeMap.size(); iNode++) {
            String[] tokens = in.readLine().split("\t");
            _nodeToIndexMap.put(
                    Integer.parseInt(tokens[0]),
                    Integer.parseInt(tokens[1])
            );
        }
        
        // Read the boolean array
        _edges = new boolean[nodes.length()][nodes.length()];
        for (int iNode = 0; iNode < nodes.length(); iNode++) {
            String line = in.readLine();
            for (int jNode = 0; jNode < nodes.length(); jNode++) {
                _edges[iNode][jNode] = (line.charAt(jNode) == '1');
            }
        }
        _reversed = false;
    }
    
    @Override
    public Iterable<Integer> adjacent(int nodeId) {

        ArrayList<Integer> edges = new ArrayList<>();

        int nodeIndex = _nodeToIndexMap.get(nodeId);
        if (_reversed) {
            for (int iNode = 0; iNode < _edges.length; iNode++) {
                if (_edges[iNode][nodeIndex]) {
                    edges.add(_indexToNodeMap.get(iNode));
                }
            }
        } else {
            for (int iNode = 0; iNode < _edges.length; iNode++) {
                if (_edges[nodeIndex][iNode]) {
                    edges.add(_indexToNodeMap.get(iNode));
                }
            }
        }
        return edges;
    }
    
    public void edge(int sourceId, int targetId) {
	
        _edges[_nodeToIndexMap.get(sourceId)][_nodeToIndexMap.get(targetId)] = true;
    }

    @Override
    public boolean hasEdge(int sourceId, int targetId) {
	
        int sourceIndex = _nodeToIndexMap.get(sourceId);
        int targetIndex = _nodeToIndexMap.get(targetId);
        if (_reversed) {
            return _edges[targetIndex][sourceIndex];
        } else {
            return _edges[sourceIndex][targetIndex];
        }
    }

    @Override
    public AdjacencyGraph reverse() {

        return new SignatureGraph(
            this.nodes(),
            _edges,
            _nodeToIndexMap,
            _indexToNodeMap
        );
    }
    
    public void write(PrintWriter out) {
        
        if (_reversed) {
            throw new RuntimeException("Cannot write reversed graph");
        }

        // Write index to node map as single line
        out.println(StringHelper.joinIntegers(_indexToNodeMap));
        
        // For each entry in the node to index map write one key value line
        for (int key : _nodeToIndexMap.keySet()) {
            out.println(key + "\t" + _nodeToIndexMap.get(key));
        }
        
        // Single string for each row in the matrix. '1' represents true and
        // '0' represents false
        for (int iNode = 0; iNode < this.nodes().length(); iNode++) {
            for (int jNode = 0; jNode < this.nodes().length(); jNode++) {
                if (_edges[iNode][jNode]) {
                    out.print("1");
                } else {
                    out.print("0");
                }
            }
            out.println();
        }
    }
}
