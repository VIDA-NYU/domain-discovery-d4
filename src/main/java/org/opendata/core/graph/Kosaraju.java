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

import java.util.Deque;
import java.util.HashMap;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.set.MutableIdentifiableIDSet;

/**
 * Find strongly connected components in directed graph using Kosaraju's
 * algorithm.
 * 
 * Adopted from:
 * http://www.keithschwarz.com/interesting/code/kosaraju/Kosaraju.java.htm
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Kosaraju {

    /**
     * Get all strongly connected components in a directed graph.
     * 
     * @param graph
     * @return 
     */
    public IdentifiableObjectSet<IdentifiableIDSet> stronglyConnectedComponents(
            AdjacencyGraph graph
    ) {

        // Nodes are visited in depth-first order
        Deque<Integer> visitOrder = graph.reverse().dfs();
	
        /* Now we can start listing connected components.  To do this, we'll
         * create the result map, as well as a counter keeping track of which
         * DFS iteration this is.
         */
        HashMap<Integer, Integer> mapping = new HashMap<>();
        int iteration = 0;

        /* Continuously process the the nodes from the queue by running a DFS
         * from each unmarked node we encounter.
         */
        while (!visitOrder.isEmpty()) {
            /* Grab the last node.  If we've already labeled it, skip it and
             * move on.
             */
            int startPoint = visitOrder.pop();
            
            if (!mapping.containsKey(startPoint)) {
                /* Run a DFS from this node, recording everything we visit as being
                 * at the current level.
                 */
                this.markReachableNodes(startPoint, graph, mapping, iteration);

                /* Bump up the number of the next SCC to label. */
                ++iteration;
            }
        }

        HashObjectSet<MutableIdentifiableIDSet> components = new HashObjectSet<>();
        for (int nodeId : mapping.keySet()) {
            int compId = mapping.get(nodeId);
            if (components.contains(compId)) {
                components.get(compId).add(nodeId);
            } else {
                components.add(new MutableIdentifiableIDSet(compId)).add(nodeId);
            }
        }
        
        HashObjectSet<IdentifiableIDSet> result = new HashObjectSet<>();
        for (IdentifiableIDSet comp : components) {
            result.add(comp);
        }
        return result;
    }

    private void markReachableNodes(
            int node,
            AdjacencyGraph g,
            HashMap<Integer, Integer> mapping,
            int label
    ) {
        if (!mapping.containsKey(node)) {
            mapping.put(node, label);
            for (int endpoint: g.adjacent(node)) {
                this.markReachableNodes(endpoint, g, mapping, label);
            }
        }
    }
}
