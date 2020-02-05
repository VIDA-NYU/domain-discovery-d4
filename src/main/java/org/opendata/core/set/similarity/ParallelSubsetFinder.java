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

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;

/**
 * Cached subset finder that computes all subsets at start in parallel.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ParallelSubsetFinder extends SubsetFinder {
    
    private class ParallelSubsetComputer implements Runnable {

        private final List<IdentifiableIDSet> _elements;
        private final int _id;
        private final ParallelSubsetFinder _computer;
        private final int _threads;
        
        public ParallelSubsetComputer(
                int id,
                int threads,
                List<IdentifiableIDSet> elements,
                ParallelSubsetFinder computer
        ) {
            _id = id;
            _threads = threads;
            _elements = elements;
            _computer = computer;
        }
        
        @Override
        public void run() {

            for (int iElement = 0; iElement < _elements.size(); iElement++) {
                if ((iElement % _threads) == _id) {
                    IdentifiableIDSet el = _elements.get(iElement);
                    _computer.put(el.id(), _computer.getSubsetsFor(el));
                }
            }
        }
    }
    
    private final HashMap<Integer, IdentifiableObjectSet<IdentifiableIDSet>> _cache;
    
    public ParallelSubsetFinder(
            IdentifiableObjectSet<IdentifiableIDSet> elements,
            int threads
    ) throws java.lang.InterruptedException {

        super(elements);
        
        _cache = new HashMap<>();
        
        List<IdentifiableIDSet> elementList = elements.toList();
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(new ParallelSubsetComputer(iThread, threads, elementList, this));
        }
        es.shutdown();
        es.awaitTermination(threads, TimeUnit.DAYS);
    }

    public synchronized void put(int id, IdentifiableObjectSet<IdentifiableIDSet> subsets) {
        
        _cache.put(id, subsets);
    }
    @Override
    public IdentifiableObjectSet<IdentifiableIDSet> getSubsetsFor(int id) {
        
        return _cache.get(id);
    }
}
