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

import java.util.Arrays;
import java.util.List;

/**
 * Iterator over the elements in a set of immutable ID sets.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class MultiSetIterator implements IDSetIterator {

    private final SingleSetIterator[] _iterators;
    
    public MultiSetIterator(List<ImmutableIDSet> elements) {
    
        _iterators = new SingleSetIterator[elements.size()];
        for (int iSet = 0; iSet < elements.size(); iSet++) {
            _iterators[iSet] = new SingleSetIterator(elements.get(iSet));
        }
        Arrays.sort(_iterators);
    }
    
    @Override
    public boolean hasNext() {

        return _iterators[0].hasNext();
    }

    @Override
    public Integer next() {

        int val = _iterators[0].next();
        // Advance all iterators that have the same value as the first
        // iterator. At this point there is no guarantee that the list
        // of iterators is sorted. Thus, we need to got through the
        // complete list.
        for (int iIndex = 1; iIndex < _iterators.length; iIndex++) {
            SingleSetIterator iter = _iterators[iIndex];
            if ((iter.hasNext()) && (iter.peek() == val)) {
                iter.next();
            }
        }
        // Bring the iterator with the smallest element to the front
        SingleSetIterator iter0 = _iterators[0];
        for (int iIndex = 1; iIndex < _iterators.length; iIndex++) {
            SingleSetIterator iterI = _iterators[iIndex];
            if (iter0.compareTo(iterI) > 0) {
                _iterators[0] = iterI;
                _iterators[iIndex] = iter0;
                iter0 = iterI;
            }
        }
        
        return val;
    }
}
