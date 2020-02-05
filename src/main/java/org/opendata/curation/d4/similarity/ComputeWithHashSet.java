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
package org.opendata.curation.d4.similarity;

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
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ComputeWithHashSet {
    
    private class OverlapComputer implements Runnable {

        private final List<EQ> _nodes;
        private final ConcurrentLinkedQueue<EQ> _queue;

        public OverlapComputer(
                ConcurrentLinkedQueue<EQ> queue,
                List<EQ> nodes
        ) {
            _queue = queue;
            _nodes = nodes;
        }
        
        @Override
        public void run() {
            
            EQ nodeI;
            while ((nodeI = _queue.poll()) != null) {
                for (EQ nodeJ : _nodes) {
                    if (nodeI.id() != nodeJ.id()) {
                        nodeI.columns().overlap(nodeJ.columns());
                    }
                }
            }
        }
    }
    
    public void run(File eqFile, int threads) throws java.io.IOException {
        
        List<EQ> nodes = new EQReader(eqFile).read().toList();
        ConcurrentLinkedQueue<EQ> queue = new ConcurrentLinkedQueue<>(nodes);
        
        ExecutorService es = Executors.newCachedThreadPool();
        
        System.out.println("START @ " + new Date());
        
        for (int iThread = 0; iThread < threads; iThread++) {
            OverlapComputer command = new OverlapComputer(queue, nodes);
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
            new ComputeWithHashSet().run(eqFile, threads);
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
