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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.opendata.core.io.FileSystem;

/**
 * Implementation of mutable identifier set using hash set.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class HashIDSet extends IDSetImpl implements MutableIDSet {

    private final HashSet<Integer> _values;
        
    public HashIDSet() {
        
        this(new HashSet<Integer>());
    }

    public HashIDSet(HashSet<Integer> values) {
        
        _values = values;
    }

    public HashIDSet(Collection<Integer> values) {
        
        this(new HashSet<>(values));
    }
    
    public HashIDSet(Iterable<Integer> values) {
        
        _values = new HashSet<>();
        for (int val : values) {
            _values.add(val);
        }
    }
    
    public HashIDSet(int[] values) {
        
        _values = new HashSet<>();
        for (int val : values) {
            _values.add(val);
        }
    }
    
    public HashIDSet(Integer[] values) {
        
        _values = new HashSet<>();
        for (Integer val : values) {
            _values.add(val);
        }
    }
    
    public HashIDSet(String[] values) {
        
        _values = new HashSet<>();
        for (String val : values) {
            _values.add(Integer.parseInt(val));
        }
    }
    
    public HashIDSet(IDSet values) {
        
        _values = new HashSet<>();
        for (int val : values) {
            _values.add(val);
        }
    }
    
    public HashIDSet(int value) {
        
        this(new int[]{value});
    }

    public HashIDSet(InputStream is) throws java.io.IOException {
    
        this();
        
        try (BufferedReader in = new BufferedReader(new InputStreamReader(is))) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                _values.add(Integer.parseInt(tokens[0]));
            }
        }
    }
    
    public HashIDSet(File file) throws java.io.IOException {
    
        this(FileSystem.openFile(file));
    }

    public HashIDSet(int start, int end) {
        
        this();
        
        for (int id = start; id < end; id++) {
            this.add(id);
        }
    }
    
    @Override
    public final void add(int nodeId) {

        _values.add(nodeId);
    }
    
    @Override
    public final void add(IDSet nodes) {

        for (int nodeId : nodes) {
            _values.add(nodeId);
        }
    }
    
    public void clear() {
        
        _values.clear();
    }
    
    @Override
    public boolean contains(Integer nodeId) {
    
        return _values.contains(nodeId);
    }

    @Override
    public HashIDSet create(Collection<Integer> values) {

        return new HashIDSet(values);
    }
    
    @Override
    public int first() {

        return _values.iterator().next();
    }
    
    @Override
    public boolean isEmpty() {

        return _values.isEmpty();
    }

    @Override
    public Iterator<Integer> iterator() {

        return _values.iterator();
    }
    
    @Override
    public int length() {
        
        return _values.size();
    }

    @Override
    public void remove(int nodeId) {

        _values.remove(nodeId);
    }

    @Override
    public boolean replace(int sourceId, int targetId) {

        if (this.contains(sourceId)) {
            _values.remove(sourceId);
            _values.add(targetId);
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public int[] toArray() {
        
        List<Integer> values = this.toSortedList();
        int[] result = new int[values.size()];
        for (int iValue = 0; iValue < values.size(); iValue++) {
            result[iValue] = values.get(iValue);
        }
        return result;
    }
    
    @Override
    public List<Integer> toList() {
    
        return new ArrayList<>(_values);
    }
    
    public static HashIDSet union(List<IDSet> sets) {
        
        if (sets.isEmpty()) {
            return new HashIDSet();
        }
        
        HashIDSet result = new HashIDSet(sets.get(0));
        for (int iSet = 1; iSet < sets.size(); iSet++) {
            result.add(sets.get(iSet));
        }
        return result;
    }
    
    public void write(File file) throws java.io.IOException {

        try (PrintWriter out = FileSystem.openPrintWriter(file)) {
            for (int nodeId : this.toSortedList()) {
            out.println(nodeId);
            }
        }
    }
}
