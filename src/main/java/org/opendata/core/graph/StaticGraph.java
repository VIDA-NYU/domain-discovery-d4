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

import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.set.ObjectSet;
import org.opendata.core.set.Signature;

/**
 * Adjacency graph with fixed set of edges.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class StaticGraph extends AdjacencyGraph {

    private final IdentifiableObjectSet<Signature> _edges;

    public StaticGraph(IdentifiableObjectSet<Signature> edges) {
        
        super(edges.keys());
        
        _edges = edges;
    }

    @Override
    public ObjectSet<Integer> adjacent(int nodeId) {

	return _edges.get(nodeId);
    }

    @Override
    public AdjacencyGraph reverse() {

	return new ReverseGraph(this);
    }    

    @Override
    public boolean hasEdge(int sourceId, int targetId) {

        return _edges.get(sourceId).contains(targetId);
    }
}
