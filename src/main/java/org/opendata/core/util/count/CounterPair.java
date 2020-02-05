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
package org.opendata.core.util.count;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class CounterPair<T> implements Comparable<CounterPair<T>> {
    
    private final Counter _counter;
    private final T _element;
    
    public CounterPair(T element, Counter counter) {
        
        _element = element;
        _counter = counter;
    }

    public CounterPair(T element, int count) {
        
        this(element, new Counter(count));
    }

    @Override
    public int compareTo(CounterPair<T> pair) {

        return Integer.compare(_counter.value(), pair.counter().value());
    }
    
    public int count() {
        
        return _counter.value();
    }

    public Counter counter() {
    
        return _counter;
    }
    
    public T element() {
        
        return _element;
    }
}
