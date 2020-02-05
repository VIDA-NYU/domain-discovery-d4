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

import org.opendata.core.object.IdentifiableObject;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.MutableIdentifiableIDSet;

/**
 * Implementation of adjacency graph that allows to add edges.
 * 
 * Uses object sets to main edges between nodes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DynamicGraph extends AdjacencyGraph {

    private final HashObjectSet<MutableIdentifiableIDSet> _edges;
    
    public DynamicGraph(IDSet nodes) {
        
        super(nodes);
        
        _edges = new HashObjectSet<>();
        for (int nodeId : nodes) {
            _edges.add(new MutableIdentifiableIDSet(nodeId));
        }
    }
    
    /**
     * Add a directed edge from the source node to the target node.
     * 
     * @param source
     * @param target 
     */
    public void add(int source, int target) {

        _edges.get(source).add(target);
    }

    /**
     * Add a directed edge from the node representing the source object to the
     * node representing the target object.
     * 
     * @param source
     * @param target 
     */
    public void add(IdentifiableObject source, IdentifiableObject target) {
        
        this.add(source.id(), target.id());
    }
    
    @Override
    public IDSet adjacent(int nodeId) {

        return _edges.get(nodeId);
    }

    @Override
    public boolean hasEdge(int sourceId, int targetId) {

        return _edges.get(sourceId).contains(targetId);
    }

    @Override
    public AdjacencyGraph reverse() {

        DynamicGraph g = new DynamicGraph(this.nodes());
        
        for (int target : this.nodes()) {
            for (int source : this.adjacent(target)) {
                g.add(source, target);
            }
        }
        
        return g;
    }
}
