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

import java.io.BufferedReader;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.opendata.core.io.FileSystem;

/**
 * Implements the IDSet interface.
 * 
 * This is an immutable IDSet using an integer array to represent the list of
 * identifier. The elements in the array are sorted in ascending order.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ImmutableIDSet extends IDSetImpl implements IDSet {

    private final Integer[] _values;
    
    /**
     * Initialize the identifier set.
     * 
     * Will sort the given list if the sorted flag is false.
     * 
     * Raises an exception if the values in the list are not unique.
     * 
     * @param values 
     * @param sorted 
     */
    public ImmutableIDSet(Collection<Integer> values, boolean sorted) {
        
        Integer[] arr = new Integer[values.size()];
        _values = values.toArray(arr);
        if (!sorted) {
            Arrays.sort(_values);
        }
        if (_values.length > 1) {
            for (int iPos = 1; iPos < _values.length; iPos++) {
                if (_values[iPos - 1].compareTo(_values[iPos]) == 0) {
                    throw new IllegalArgumentException("Duplicate ID: " + _values[iPos]);
                }
            }
        }
    }
    
    public ImmutableIDSet(Collection<Integer> values) {

        this(values, false);
    }
    
    public ImmutableIDSet(IDSet values) {

        this(values.toList(), false);
    }
    
    public ImmutableIDSet(Integer[] values, boolean sorted) {
        
        _values = values;
        if (!sorted) {
            Arrays.sort(_values);
        }
        if (_values.length > 1) {
            for (int iPos = 1; iPos < _values.length; iPos++) {
                if (_values[iPos - 1].compareTo(_values[iPos]) == 0) {
                    throw new IllegalArgumentException("Duplicate ID: " + _values[iPos]);
                }
            }
        }
    }

    public ImmutableIDSet(Integer[] values) {
        
        this(values, false);
    }

    public ImmutableIDSet(String values) {

        if (values.equals("")) {
            _values = new Integer[0];
        } else {
            String[] tokens = values.split(",");
            _values = new Integer[tokens.length];
            _values[0] = Integer.parseInt(tokens[0]);
            for (int iPos = 1; iPos < _values.length; iPos++) {
                _values[iPos] = Integer.parseInt(tokens[iPos]);
                if (_values[iPos - 1].compareTo(_values[iPos]) >= 0) {
                    throw new IllegalArgumentException("Not a unique ID list: " + values);
                }
            }
        }
    }
    
    public ImmutableIDSet(int value) {
        
        _values = new Integer[]{value};
    }
    
    public ImmutableIDSet(int minId, int maxId) {
    
        _values = new Integer[maxId - minId];
        for (int iValue = 0; iValue < _values.length; iValue++) {
            _values[iValue] = minId + iValue;
        }
    }
    
    public ImmutableIDSet(File file) throws java.io.IOException {
        
        HashIDSet values = new HashIDSet();
        
        try (BufferedReader in = FileSystem.openReader(file)) {
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (!line.equals("")) {
                    for (String token : line.split(",")) {
                        values.add(Integer.parseInt(token));
                    }
                }
            }
        }
        Integer[] arr = new Integer[values.length()];
        _values = values.toList().toArray(arr);
        Arrays.sort(_values);
    }
    
    public ImmutableIDSet() {
        
        _values = new Integer[0];
    }
    
    @Override
    public boolean contains(Integer nodeId) {

        return (Arrays.binarySearch(_values, nodeId) >= 0);
    }

    @Override
    public ImmutableIDSet create(Collection<Integer> values) {

        return new ImmutableIDSet(values);
    }

    @Override
    public int first() {

        return _values[0];
    }

    @Override
    public boolean isEmpty() {

        return (_values.length == 0);
    }

    @Override
    public Iterator<Integer> iterator() {
        
        return Arrays.asList(_values).iterator();
    }

    public int get(int index) {
	
	return _values[index];
    }
    
    @Override
    public int length() {

        return _values.length;
    }

    @Override
    public boolean replace(int sourceId, int targetId) {

        for (int iValue = 0; iValue < _values.length; iValue++) {
            if (_values[iValue] == sourceId) {
                _values[iValue] = targetId;
                Arrays.sort(_values);
                return true;
            }
        }
        return false;
    }

    public int sortedOverlap(ImmutableIDSet nodes, HashMap<Integer, Integer> nodesSize) {

        int indexI = 0;
        int indexJ = 0;

        final int len = nodes.length();
	
        int ovp = 0;
        while ((indexI < _values.length) && (indexJ < len)) {
            final int nodeId = _values[indexI];
            final int comp = Integer.compare(nodeId, nodes.get(indexJ));
            if (comp < 0) {
                indexI++;
            } else if (comp > 0) {
                indexJ++;
            } else {
                indexI++;
                indexJ++;
                if (nodesSize != null) {
                    ovp += nodesSize.get(nodeId);
                } else {
                    ovp++;
                }
            }
        }
        return ovp;
    }

    public int sortedOverlap(ImmutableIDSet nodes) {

        return this.sortedOverlap(nodes, null);
    }
    
    public ImmutableIDSet trim(ImmutableIDSet nodes) {

        final int ovp = this.sortedOverlap(nodes);
        if (ovp == 0) {
            return new ImmutableIDSet();
        } else {
            Integer[] values = new Integer[ovp];
            int indexI = 0;
            int indexJ = 0;
            int index = 0;
            while (index < ovp) {
                final int val = _values[indexI];
                final int comp = Integer.compare(val, nodes.get(indexJ));
                if (comp < 0) {
                    indexI++;
                } else if (comp > 0) {
                    indexJ++;
                } else {
                    indexI++;
                    indexJ++;
                    values[index++] = val;
                }
            }
            return new ImmutableIDSet(values, true);
        }
    }
    
    @Override
    public int[] toArray() {
        
        int[] result = new int[_values.length];
        for (int iValue = 0; iValue < _values.length; iValue++) {
            result[iValue] = _values[iValue];
        }
        return result;
    }
    
    @Override
    public List<Integer> toList() {

        return Arrays.asList(_values);
    }
}
