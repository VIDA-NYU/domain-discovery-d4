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
package org.opendata.db.eq;

import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.graph.UndirectedConnectedComponents;
import org.opendata.core.io.FileSystem;
import org.opendata.core.metric.JaccardIndex;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.sort.IdentifiableObjectSort;

/**
 * Generate a compressed term index where similar equivalence classes are
 * merged.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimilarTermIndexGenerator {
    
    private class OverlapComputer implements Runnable {

        private final UndirectedConnectedComponents _graph;
        private final ConcurrentLinkedQueue<Node> _queue;
        private final List<Node> _nodes;
        private final Threshold _threshold;

        public OverlapComputer(
                ConcurrentLinkedQueue<Node> queue,
                List<Node> nodes,
                Threshold threshold,
                UndirectedConnectedComponents graph
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
                                    .sim(nodeI.columnCount(), nodeJ.columnCount(), overlap);
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
    
    public void run(EQIndex eqIndex, Threshold threshold, int threads, PrintWriter out) {
        
        UndirectedConnectedComponents graph;
        graph = new UndirectedConnectedComponents(eqIndex.nodes().keys());
        
        List<Node> nodes = eqIndex.nodes().toList();
        ConcurrentLinkedQueue<Node> queue = new ConcurrentLinkedQueue<>(nodes);
        
        Collections.sort(nodes, new IdentifiableObjectSort<>());
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(new OverlapComputer(queue, nodes, threshold, graph));
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        
        for (IdentifiableIDSet comp : graph.getComponents()) {
            if (comp.length() == 1) {
                eqIndex.get(comp.first()).write(out);
            } else {
                HashIDSet columns = new HashIDSet();
                HashIDSet terms = new HashIDSet();
                for (int nodeId : comp) {
                    EQ node = eqIndex.get(nodeId);
                    columns.add(node.columns());
                    terms.add(node.terms());
                }
                new EQImpl(comp.id(), terms, columns).write(out);
            }
        }
    }
    
    private final static String COMMAND =
            "Usage:\n  <eq-file>\n  <threshold>\n  <threads>\n  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(SimilarTermIndexGenerator.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        Threshold threshold = Threshold.getConstraint(args[1]);
        int threads = Integer.parseInt(args[2]);
        File outFile = new File(args[3]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outFile)) {
            new SimilarTermIndexGenerator()
                    .run(new EQIndex(eqFile), threshold, threads, out);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
