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
package org.opendata.core.similarity;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.opendata.core.object.Entity;

/**
 * Compute pairwise similarity between string entities in parallel.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ParallelStringSimilarityComputer {
    
    private class SimilarityComputeTask implements Runnable {

        private final ObjectSimilarityConsumer _consumer;
        private final StringSimilarityComputer _func;
        private final ConcurrentLinkedQueue<Entity> _queue;
        private final List<Entity> _terms;
        
        public SimilarityComputeTask(
                ConcurrentLinkedQueue<Entity> queue,
                List<Entity> terms,
                StringSimilarityComputer func,
                ObjectSimilarityConsumer consumer
        ) {
            _queue = queue;
            _terms = terms;
            _func = func;
            _consumer = consumer;
        }
        
        @Override
        public void run() {

            int count = 0;
            
            Entity term;
            while ((term = _queue.poll()) != null) {
                for (Entity t1 : _terms) {
                    if (term.id() < t1.id()) {
                        BigDecimal sim = _func.sim(term.name(), t1.name());
                        if (sim != null) {
                            _consumer.consume(term, t1, sim);
                        }
                    }
                }
                count++;
                if ((count % 1000) == 0) {
                    System.out.println(count + " @ " + new java.util.Date());
                }
            }
        }
    }
    
    public void run(
            List<Entity> terms,
            StringSimilarityComputer func,
            int threads,
            ObjectSimilarityConsumer consumer
    ) throws java.lang.InterruptedException {
        
        
        ConcurrentLinkedQueue<Entity> queue;
        queue = new ConcurrentLinkedQueue<>(terms);
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(
                    new SimilarityComputeTask(
                            queue,
                            terms,
                            func,
                            consumer
                    )
            );
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.DAYS);
    }
}
