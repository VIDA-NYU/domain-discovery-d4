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
package org.opendata.core.set;

/**
 * Iterator for a single immutable ID Set.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SingleSetIterator implements Comparable<SingleSetIterator>, IDSetIterator {
   
    private boolean _hasNext;
    private int _readIndex;
    private final ImmutableIDSet _values;

    public SingleSetIterator(ImmutableIDSet values) {

        _values = values;

        _hasNext = !values.isEmpty();
        _readIndex = 0;
    }

    @Override
    public int compareTo(SingleSetIterator iter) {

        if (!_hasNext) {
            if (iter.hasNext()) {
                return 1;
            } else {
                return 0;
            }
        } else if (!iter.hasNext()) {
            return -1;
        } else {
            return Integer.compare(this.peek(), iter.peek());
        }
    }

    @Override
    public boolean hasNext() {

        return _hasNext;
    }

    @Override
    public Integer next() {

        int val = _values.get(_readIndex++);
        _hasNext = (_readIndex < _values.length());
        return val;
    }

    public Integer peek() {

        return _values.get(_readIndex);
    }
}
