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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.metric.JaccardIndex;
import org.opendata.core.sort.IdentifiableObjectSort;

/**
 * Create a graph where edges between nodes exist of the nodes are similar above
 * a given threshold in their element list. Allows to compute the graph in parallel
 * using multiple threads.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ParallelOverlapGraphGenerator {
    
    private class OverlapComputer implements Runnable {

        private final GraphGenerator _graph;
        private final ConcurrentLinkedQueue<Node> _queue;
        private final List<Node> _nodes;
        private final Threshold _threshold;

        public OverlapComputer(
                ConcurrentLinkedQueue<Node> queue,
                List<Node> nodes,
                Threshold threshold,
                GraphGenerator graph
        ) {
            _queue = queue;
            _nodes = nodes;
            _threshold = threshold;
            _graph = graph;
        }
        
        @Override
        public void run() {

            Node nodeI;
            while ((nodeI = _queue.poll()) != null) {
                for (int jNode = 0; jNode < _nodes.size(); jNode++) {
                    Node nodeJ = _nodes.get(jNode);
                    if (nodeJ.id() < nodeI.id()) {
                        int overlap = nodeI.overlap(nodeJ);
                        if (overlap > 0) {
                            BigDecimal sim = new JaccardIndex()
                                    .sim(
                                            nodeI.elementCount(),
                                            nodeJ.elementCount(),
                                            overlap
                                    );
                            if (_threshold.isSatisfied(sim)) {
                                _graph.edge(nodeI.id(), nodeJ.id());
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }
    
    /**
     * Compute similarity edges between nodes in a graph in parallel.
     * 
     * @param nodes
     * @param threshold
     * @param threads
     * @param graph 
     */
    public void run(
            List<Node> nodes,
            Threshold threshold,
            int threads,
            GraphGenerator graph
    ) {
        // Sort nodes by their identifier.
        ArrayList<Node> sortedNodes = new ArrayList<>(nodes);
        Collections.sort(sortedNodes, new IdentifiableObjectSort<>());
        
        ConcurrentLinkedQueue<Node> queue = new ConcurrentLinkedQueue<>(nodes);
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(new OverlapComputer(queue, sortedNodes, threshold, graph));
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
}
