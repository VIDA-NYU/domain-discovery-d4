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
package org.opendata.db.tools;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.opendata.core.io.SynchronizedWriter;
import org.opendata.core.object.IdentifiableArray;
import org.opendata.core.util.ArrayHelper;

/**
 * Compute pairwise similarity between identifiable sets.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class OverlapComputer {
    
    private class SimilarityComputeTask implements Runnable {

        private final Collection<IdentifiableArray> _elements;
        private final ConcurrentLinkedQueue<IdentifiableArray> _queue;
        private final int[] _nodeSizes;
        private final SynchronizedWriter _out;
        
        public SimilarityComputeTask(
                Collection<IdentifiableArray> elements,
                ConcurrentLinkedQueue<IdentifiableArray> queue,
                int[] nodeSizes,
                SynchronizedWriter out
        ) {
            _elements = elements;
            _queue = queue;
            _nodeSizes = nodeSizes;
            _out = out;
        }
        
        @Override
        public void run() {

        	IdentifiableArray elementI;
            while ((elementI = _queue.poll()) != null) {
                for (IdentifiableArray elementJ : _elements) {
                    if (elementI.id() < elementJ.id()) {
                        int overlap = ArrayHelper.overlap(
                        		elementI.values(),
                        		elementJ.values(),
                        		_nodeSizes
                        );
                        if (overlap > 0) {
                        	_out.write(
                        			String.format(
                        					"%d\t%d\t%d",
                        					elementI.id(),
                        					elementJ.id(),
                        					overlap
                        				)
                        		);
                        }
                    }
                }
            }
        }
    }
    
    public void run(
            List<IdentifiableArray> elements,
            int[] nodeSizes,
            int threads,
            SynchronizedWriter out
    ) throws java.lang.InterruptedException {
        
        ConcurrentLinkedQueue<IdentifiableArray> queue = new ConcurrentLinkedQueue<>(elements);
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(new SimilarityComputeTask(elements, queue, nodeSizes, out));
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.DAYS);
    }    
}
