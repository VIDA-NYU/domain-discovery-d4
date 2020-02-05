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
package org.opendata.curation.d4.signature;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQReader;

/**
 * Compute context signatures in parallel using multiple threads.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ParallelContextSignatureComputer {
    
    private class OverlapComputer implements Runnable {

        private final int[][] _nodes;
        private final int _nodeCount;
        private final ConcurrentLinkedQueue<Integer> _queue;
        
        public OverlapComputer(
                ConcurrentLinkedQueue<Integer> queue,
                int[][] nodes,
                int nodeCount
        ) {
            _queue = queue;
            _nodes = nodes;
            _nodeCount = nodeCount;
        }
        
        @Override
        public void run() {
            
            Integer iNode;
            while ((iNode = _queue.poll()) != null) {
                for (int jNode = 0; jNode < _nodeCount; jNode++) {
                    if (iNode != jNode) {
                        final int[] colI = _nodes[iNode];
                        final int[] colJ = _nodes[jNode];
                        final int lenI = colI.length;
                        final int lenJ = colJ.length;
                        int idxI = 0;
                        int idxJ = 0;
                        int overlap = 0;
                        while ((idxI < lenI) && (idxJ < lenJ)) {
                            if (colI[idxI] < colJ[idxJ]) {
                                idxI++;
                            } else if (colI[idxI] > colJ[idxJ]) {
                                idxJ++;
                            } else {
                                overlap++;
                                idxI++;
                                idxJ++;
                            }
                        }
                        if (overlap > 0) {
                            System.out.println(iNode + "\t" + jNode + "\t" + overlap);
                        }
                    }
                }
            }
        }
    }
    
    public void run(File eqFile, int threads) throws java.io.IOException {
        
        List<EQ> nodes = new EQReader(eqFile).read().toList();
        
        ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<>();
        
        int[][] nodeColumns = new int[nodes.size()][];
        for (int iEQ = 0; iEQ < nodes.size(); iEQ++) {
            nodeColumns[iEQ] = nodes.get(iEQ).columns().toArray();
            queue.add(iEQ);
        }
        
        ExecutorService es = Executors.newCachedThreadPool();
        
        System.out.println("START @ " + new Date());
        
        for (int iThread = 0; iThread < threads; iThread++) {
            OverlapComputer command = new OverlapComputer(
                    queue,
                    nodeColumns,
                    nodes.size()
            );
            es.execute(command);
        }
        es.shutdown();
        try {
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        System.out.println("END @ " + new Date());
    }
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println("Usage: <eq-file> <threads>");
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        int threads = Integer.parseInt(args[1]);
        
        try {
            new ParallelContextSignatureComputer().run(eqFile, threads);
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
