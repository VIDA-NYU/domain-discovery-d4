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

import com.google.gson.JsonArray;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import org.opendata.core.metric.JaccardIndex;
import org.opendata.core.object.IdentifiableObjectImpl;

/**
 * Implementation for identifiable identifier set that contains a immutable set
 * of identifiers.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IdentifiableIDSetWrapper extends IdentifiableObjectImpl implements IdentifiableIDSet {
    
    private final IDSet _values;
    
    public IdentifiableIDSetWrapper(int id) {
    
        super(id);
        
        _values = new ImmutableIDSet();
    }
    
    public IdentifiableIDSetWrapper(int id, IDSet values) {
        
        super(id);
        
        _values = values;
    }

    @Override
    public boolean contains(Integer element) {

        return _values.contains(element);
    }

    @Override
    public boolean contains(ObjectSet<Integer> values) {

        return _values.contains(values);
    }

    @Override
    public IDSet difference(IDSet list) {

        return _values.difference(list);
    }

    @Override
    public IDSet difference(int id) {

        return _values.difference(id);
    }

    @Override
    public int first() {

        return _values.first();
    }

    @Override
    public IDSet intersect(IDSet list) {

        return _values.intersect(list);
    }

    @Override
    public boolean isEmpty() {

        return _values.isEmpty();
    }

    @Override
    public boolean isTrueSubsetOf(ObjectSet<Integer> list) {

        return _values.isTrueSubsetOf(list);
    }

    @Override
    public boolean isTrueSubsetOf(IDSet list) {

        return _values.isTrueSubsetOf(list);
    }

    @Override
    public Iterator<Integer> iterator() {

        return _values.iterator();
    }
    
    @Override
    public BigDecimal ji(IDSet list) {
    
        return new JaccardIndex()
        		.sim(this.length(), list.length(), this.overlap(list));
    }

    @Override
    public int length() {

        return _values.length();
    }

    @Override
    public int maxId() {

        return _values.maxId();
    }

    @Override
    public int minId() {

        return _values.minId();
    }

    @Override
    public int overlap(ObjectSet<Integer> list) {

        return _values.overlap(list);
    }

    @Override
    public boolean overlaps(ObjectSet<Integer> list) {

        return _values.overlaps(list);
    }
    
    @Override
    public boolean overlaps(IDSet set, int threshold) {
        
        return _values.overlaps(set, threshold);
    }

    @Override
    public boolean replace(int sourceId, int targetId) {

        return _values.replace(sourceId, targetId);
    }

    @Override
    public boolean sameSetAs(ObjectSet<Integer> list) {

        return _values.sameSetAs(list);
    }

    @Override
    public IDSet sample(int size) {

        return _values.sample(size);
    }

    @Override
    public int[] toArray() {

        return _values.toArray();
    }

    @Override
    public String toIntString() {

        return _values.toIntString();
    }

    @Override
    public JsonArray toJsonArray() {

        return _values.toJsonArray();
    }

    @Override
    public List<Integer> toList() {

        return _values.toList();
    }

    @Override
    public List<Integer> toSortedList() {

        return _values.toSortedList();
    }

    @Override
    public IDSet union(IDSet list) {

        return _values.union(list);
    }

    @Override
    public IDSet union(int id) {

        return _values.union(id);
    }
}
