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
package org.opendata.core.set.similarity;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.similarity.ObjectSimilarityConsumer;

/**
 * Compute pairwise similarity between identifiable sets.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class ParallelSetSimilarityComputer <T extends IdentifiableIDSet> {
    
    private class SimilarityComputeTask  <T extends IdentifiableIDSet> implements Runnable {

        private final ObjectSimilarityConsumer _consumer;
        private final Collection<T> _elements;
        private final ConcurrentLinkedQueue<T> _queue;
        private final SetSimilarityComputer<T> _sim;
        
        public SimilarityComputeTask(
                Collection<T> elements,
                ConcurrentLinkedQueue<T> queue,
                SetSimilarityComputer<T> sim,
                ObjectSimilarityConsumer consumer
        ) {
            _elements = elements;
            _queue = queue;
            _sim = sim;
            _consumer = consumer;
        }
        
        @Override
        public void run() {

            T elementI;
            while ((elementI = _queue.poll()) != null) {
                for (T elementJ : _elements) {
                    if (elementI.id() < elementJ.id()) {
                        _consumer.consume(
                                elementI,
                                elementJ,
                                _sim.getSimilarity(elementI, elementJ)
                        );
                    }
                }
            }
        }
    }
    
    public void run(
            Collection<T> elements,
            SetSimilarityComputer<T> sim,
            int threads,
            ObjectSimilarityConsumer consumer
    ) throws java.lang.InterruptedException {
        
        ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>(elements);
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(
                    new SimilarityComputeTask(
                            elements,
                            queue,
                            sim,
                            consumer
                    )
            );
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.DAYS);
    }    
}
