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
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.util.count.IdentifiableCounterSet;

/**
 * Implementation of adjacency graph that allows to add edges.
 * 
 * Uses object sets to main edges between nodes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DynamicGraph extends AdjacencyGraph {

    private final HashMap<Integer, int[]> _edges;
    
    public DynamicGraph(IDSet nodes) {
        
        super(nodes);
        
        _edges = new HashMap<>();
    }
    
    public void add(int nodeId, int[] edges) {

        _edges.put(nodeId, edges);
    }
    
    @Override
    public Iterable<Integer> adjacent(int nodeId) {

        if (_edges.containsKey(nodeId)) {
            return new HashIDSet(_edges.get(nodeId));
        } else {
            return new HashIDSet();
        }
    }

    @Override
    public AdjacencyGraph reverse() {

        IdentifiableCounterSet edgeCounts = new IdentifiableCounterSet();
        for (int target : this.nodes()) {
            for (int source : this.adjacent(target)) {
                edgeCounts.inc(source);
            }
        }
        
        HashMap<Integer, int[]> edges = new HashMap<>();
        for (int nodeId : edgeCounts.keys()) {
            edges.put(nodeId, new int[edgeCounts.get(nodeId).value()]);
        }
        
        IdentifiableCounterSet edgeIndex = new IdentifiableCounterSet();
        for (int target : this.nodes()) {
            for (int source : this.adjacent(target)) {
                edges.get(source)[edgeIndex.get(source).value()] = target;
                edgeIndex.inc(source);
            }
        }
        
        DynamicGraph g = new DynamicGraph(this.nodes());
        for (int nodeId : edges.keySet()) {
            g.add(nodeId, edges.get(nodeId));
        }
        return g;
    }
}
