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
import java.util.Iterator;
import java.util.List;
import org.opendata.core.object.IdentifiableObject;
import org.opendata.core.sort.IdentifiableObjectSort;

/**
 * Implements an immutable list of identifiable objects.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class ImmutableObjectSet <T extends IdentifiableObject> extends IdentifiableObjectSetImpl<T>  implements IdentifiableObjectSet<T> {

    private final T[] _elements;
    private HashIDSet _keys = null;
    
    public ImmutableObjectSet(T[] elements, boolean sorted) {
        
       _elements = elements;
        if (!sorted) {
            Arrays.sort(_elements, new IdentifiableObjectSort<>());
        }
        if (_elements.length > 1) {
            for (int iPos = 1; iPos < _elements.length; iPos++) {
                if (Integer.compare(_elements[iPos - 1].id(), _elements[iPos].id()) == 0) {
                    throw new IllegalArgumentException("Duplicate ID: " + _elements[iPos].id());
                }
            }
        }
    }
    
    public ImmutableObjectSet(T[] elements) {
        
        this(elements, false);
    }
    
    private int binarySearch(int key) {
        
        int low = 0;
        int high = _elements.length - 1;
        
        while (low <= high) {
            int mid = (low + high) / 2;
            int comp = Integer.compare(_elements[mid].id(), key);
            if (comp < 0) {
                low = mid + 1;
            } else if (comp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        return -1;
    }

    @Override
    public boolean contains(Integer id) {

        return (this.binarySearch(id) >= 0);
    }

    @Override
    public T get(int objectId) {

        int index = this.binarySearch(objectId);
        if (index >= 0) {
            return _elements[index];
        } else {
            throw new IllegalArgumentException("Unknown object: " + objectId);
        }
    }
    
    @Override
    public T get(IdentifiableObject obj) {
	
	return this.get(obj.id());
    }

    @Override
    public boolean isEmpty() {

        return (_elements.length == 0);
    }

    @Override
    public Iterator<T> iterator() {

        return this.toList().iterator();
    }

    @Override
    public IDSet keys() {

        if (_keys == null) {
            _keys = new HashIDSet();
            for (T obj : _elements) {
                _keys.add(obj.id());
            }
        }
        return _keys;
    }

    @Override
    public int length() {

        return _elements.length;
    }

    @Override
    public List<T> toList() {

        return Arrays.asList(_elements);
    }    
}
