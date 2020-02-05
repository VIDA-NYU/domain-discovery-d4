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

import java.util.List;
import org.opendata.core.object.IdentifiableObject;

/**
 * Thread for merging domains based on a given merge function.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class GraphBuilderTask <T extends IdentifiableObject> implements Runnable {

    private final GraphBuilderEdgeCondition _edgeCondition;
    private final GraphBuilder _graph;
    private final List<T> _nodes;
    private final int _id;
    private final int _threads;
    
    public GraphBuilderTask(
            int id,
            int threads,
            List<T> nodes,
            GraphBuilder graph,
            GraphBuilderEdgeCondition edgeCondition            
    ) {
        _id = id;
        _threads = threads;
        _nodes = nodes;
        _edgeCondition = edgeCondition;
        _graph = graph;
    }
    
    public GraphBuilderTask(
            List<T> nodes,
            GraphBuilder graph,
            GraphBuilderEdgeCondition edgeCondition
    ) {
        this(0, 1, nodes, graph, edgeCondition);
    }
    
    @Override
    public void run() {

        int count = 0;
        for (int iNode = 0; iNode < _nodes.size() - 1; iNode++) {
            if ((iNode % _threads) == _id) {
                T nodeI = _nodes.get(iNode);
                for (int jNode = iNode + 1; jNode < _nodes.size(); jNode++) {
                    T nodeJ = _nodes.get(jNode);
                    boolean hasEdge = _edgeCondition.hasEdge(nodeI.id(), nodeJ.id());
                    if (hasEdge) {
                        _graph.edge(nodeI.id(), nodeJ.id());
                    }
                    if ((_graph.isDirected()) || ((!hasEdge) && (!_edgeCondition.isSymmetric()))) {
                        if ((hasEdge) && (_edgeCondition.isSymmetric())) {
                            _graph.edge(nodeJ.id(), nodeI.id());
                        } else if (_edgeCondition.hasEdge(nodeJ.id(), nodeI.id())) {
                            _graph.edge(nodeJ.id(), nodeI.id());
                        }
                    }
                }
                count++;
            }
	}
        if ((_id != 0) || (_threads != 1)) {
            System.out.println("GRAPH BUILDER " + _id + " (" + count + ") @ " + new java.util.Date());
        }
    }
}
