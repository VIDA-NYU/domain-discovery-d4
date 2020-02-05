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
package org.opendata.core.graph.build;

import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;

/**
 * Check whether two nodes can be merge based on whether one is contained in
 * the other.
 * 
 * Use this filter to allow merging only of noes that are not contained in
 * each other.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class NonSubsetCondition <T extends IdentifiableIDSet> implements GraphBuilderEdgeCondition {

    private final IdentifiableObjectSet<T> _nodes;
    
    public NonSubsetCondition(IdentifiableObjectSet<T> nodes) {
    
        _nodes = nodes;
    }
    
    @Override
    public boolean hasEdge(int sourceId, int targetId) {

	T nodesI = _nodes.get(sourceId);
	T nodesJ = _nodes.get(targetId);
	
	int overlap = nodesI.overlap(nodesJ);
	return ((overlap != nodesI.length()) && (overlap != nodesJ.length()));
    }

    @Override
    public boolean isSymmetric() {

        return true;
    }
}
