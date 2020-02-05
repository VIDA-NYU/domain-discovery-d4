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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import org.opendata.core.object.filter.ObjectFilter;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.MutableIDSet;

/**
 * Class for adjacency graphs.
 * 
 * Directed graph of nodes. Each node is identified by a unique identifier.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class AdjacencyGraph implements ObjectFilter<Integer>, Iterable<Integer> {
    
    private final IDSet _nodes;
    
    public AdjacencyGraph(IDSet nodes) {
	
	_nodes = nodes;
    }
    
    /**
     * Get list of adjacent nodes for node with given identifier.
     * 
     * @param nodeId
     * @return 
     */
    public abstract Iterable<Integer> adjacent(int nodeId);
    
    @Override
    public boolean contains(Integer id) {
        
        return _nodes.contains(id);
    }
    
    /**
     * Queue containing the nodes of that graph in the order in which a DFS of
     * that graph visits the nodes.
     * 
     * @return 
     */
    public Deque<Integer> dfs() {
        
        // The resulting ordering of the nodes
        Deque<Integer> result = new ArrayDeque<>();

        // The set of nodes that we've visited so far
        HashIDSet visited = new HashIDSet();

        // DFS from each node
        for (int nodeId : this) {
            this.explore(nodeId, result, visited);
        }

        return result;
    }

    /**
     * Recursively explores the given node with a DFS. Adds the node to the
     * output list once the exploration of all its adjacent nodes is complete.
     * 
     * @param nodeId
     * @param result
     * @param visited 
     */
    private void explore(int nodeId, Deque<Integer> result, MutableIDSet visited) {
    
        if (!visited.contains(nodeId)) {
            // Mark node as visited
            visited.add(nodeId);
            // Recursively explore all adjacent nodes
            for (int target: this.adjacent(nodeId)) {
                this.explore(target, result, visited);
            }
            // Done exploring. Push node to result
            result.push(nodeId);
        }
    }

    public abstract boolean hasEdge(int sourceId, int targetId);
    
    @Override
    public Iterator<Integer> iterator() {
    
        return _nodes.iterator();
    }
    
    /**
     * List of identifier for all nodes in the graph.
     * 
     * @return 
     */
    public IDSet nodes() {
	
	return _nodes;
    }

    /**
     * Get the reversed directed graph.
     * 
     * @return 
     */
    public abstract AdjacencyGraph reverse();
}
