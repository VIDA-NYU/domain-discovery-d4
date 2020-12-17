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
package org.opendata.curation.d4.experiments;

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
import org.opendata.core.io.FileSystem;
import org.opendata.core.io.SynchronizedWriter;
import org.opendata.core.metric.JaccardIndex;
import org.opendata.core.sort.IdentifiableObjectSort;
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.eq.Node;

/**
 * Generate a compressed term index where similar equivalence classes are
 * merged.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQSimilarityWriter {
    
    private class OverlapComputer implements Runnable {

        private final ConcurrentLinkedQueue<Node> _queue;
        private final List<Node> _nodes;
        private final SynchronizedWriter _writer;

        public OverlapComputer(
                ConcurrentLinkedQueue<Node> queue,
                List<Node> nodes,
                SynchronizedWriter writer
        ) {
            _queue = queue;
            _nodes = nodes;
            _writer = writer;
        }
        
        @Override
        public void run() {

            Node nodeI;
            while ((nodeI = _queue.poll()) != null) {
                BigDecimal maxSim = BigDecimal.ZERO;
                for (int jNode = 0; jNode < _nodes.size(); jNode++) {
                    Node nodeJ = _nodes.get(jNode);
                    if (nodeI.id() != nodeJ.id()) {
                        int overlap = nodeI.overlap(nodeJ);
                        if (overlap > 0) {
                            BigDecimal sim = new JaccardIndex()
                                    .sim(nodeI.columnCount(), nodeJ.columnCount(), overlap);
                            if (maxSim.compareTo(sim) < 0) {
                                maxSim = sim;
                            }
                        }
                    }
                }
                _writer.write(
                        String.format(
                                "%d\t%s\t%d\t%d",
                                nodeI.id(),
                                new FormatedBigDecimal(maxSim).toString(),
                                nodeI.columnCount(),
                                nodeI.termCount()
                        )
                );
            }
        }
    }
    
    public void run(EQIndex eqIndex, int threads, PrintWriter out) {
        
        List<Node> nodes = eqIndex.nodes().toList();
        ConcurrentLinkedQueue<Node> queue = new ConcurrentLinkedQueue<>(nodes);
        
        Collections.sort(nodes, new IdentifiableObjectSort<>());
        
        SynchronizedWriter writer = new SynchronizedWriter(out);
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(new OverlapComputer(queue, nodes, writer));
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private final static String COMMAND =
            "Usage:\n  <eq-file>\n  <threads>\n  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(EQSimilarityWriter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        int threads = Integer.parseInt(args[1]);
        File outFile = new File(args[2]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outFile)) {
            new EQSimilarityWriter()
                    .run(new EQIndex(eqFile), threads, out);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
