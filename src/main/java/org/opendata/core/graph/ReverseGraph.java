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

import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;

/**
 * Reversed adjacency graph.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ReverseGraph extends AdjacencyGraph {

    private final AdjacencyGraph _g;
    
    public ReverseGraph(AdjacencyGraph g) {
    
        super(g.nodes());
	
        _g = g;
    }
    
    @Override
    public IDSet adjacent(int nodeId) {

	HashIDSet edges = new HashIDSet();
	for (int target : this.nodes()) {
	    if (_g.hasEdge(target, nodeId)) {
		edges.add(target);
	    }
	}
	return edges;
    }

    @Override
    public boolean hasEdge(int sourceId, int targetId) {

	return _g.hasEdge(targetId, sourceId);
    }

    @Override
    public AdjacencyGraph reverse() {

        return _g;
    }
}
