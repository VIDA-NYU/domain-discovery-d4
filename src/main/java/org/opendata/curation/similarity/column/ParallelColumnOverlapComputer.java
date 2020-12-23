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
package org.opendata.curation.similarity.column;

import java.io.File;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.SortedIDList;
import org.opendata.curation.d4.Constants;
import org.opendata.db.eq.EQFileReader;

/**
 * Compute overlap between database columns. Uses multiple threads to compute
 * column overlap in parallel.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ParallelColumnOverlapComputer {
    
    /**
     * Worker task for column overlap computation.
     * 
     */
    private class OverlapComputer implements Runnable {

        private final HashMap<Integer, SortedIDList> _columns;
        private final ColumnOverlapConsumer _consumer;
        private final ConcurrentLinkedQueue<Integer> _queue;
        
        public OverlapComputer(
                ConcurrentLinkedQueue<Integer> queue,
                HashMap<Integer, SortedIDList> columns,
                ColumnOverlapConsumer consumer
        ) {
            _queue = queue;
            _columns = columns;
            _consumer = consumer;
        }
        
        @Override
        public void run() {
            
            Integer iColumn;
            while ((iColumn = _queue.poll()) != null) {
                final SortedIDList colI = _columns.get(iColumn);
                for (int jColumn : _columns.keySet()) {
                    if (iColumn < jColumn) {
                        _consumer.consume(
                                iColumn,
                                jColumn,
                                colI.overlap(_columns.get(jColumn))
                        );
                    }
                }
            }
        }
    }
    
    public void run(
            HashMap<Integer, SortedIDList> columns,
            int threads,
            File outputFile
    ) throws java.io.IOException {
        
        ConcurrentLinkedQueue<Integer> queue;
        queue = new ConcurrentLinkedQueue<>(columns.keySet());
        
        ExecutorService es = Executors.newCachedThreadPool();
        
        System.out.println("START @ " + new Date());
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            ColumnOverlapWriter writer = new ColumnOverlapWriter(out);
            for (int iThread = 0; iThread < threads; iThread++) {
                OverlapComputer command = new OverlapComputer(
                        queue,
                        columns,
                        writer
                );
                es.execute(command);
            }
            es.shutdown();
            try {
                es.awaitTermination(threads, TimeUnit.DAYS);
            } catch (java.lang.InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
        System.out.println("END @ " + new Date());
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <threads>\n" +
            "  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(ParallelColumnOverlapComputer.class.getName());
    
    public static void main(String[] args) {
        
        String name = Constants.NAME + " - Column Overlap Computer ";
        System.out.println(name + " - Version (" + Constants.VERSION + ")\n");

        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        int threads = Integer.parseInt(args[1]);
        File outputFile = new File(args[2]);
        
        try {
            new ParallelColumnOverlapComputer()
                    .run(new EQFileReader(eqFile).getEQColumns(), threads, outputFile);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
