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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.opendata.core.object.IdentifiableObject;

/**
 * Implements identifiable object set using HashMap.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class HashObjectSet <T extends IdentifiableObject> extends IdentifiableObjectSetImpl<T> implements MutableObjectSet<T> {

    private final HashMap<Integer, T> _elements;
    
    public HashObjectSet() {
        
        _elements = new HashMap<>();
    }
    
    public HashObjectSet(T element) {
        
        _elements = new HashMap<>();
        _elements.put(element.id(), element);
    }
    
    public HashObjectSet(Iterable<T> elements) {
        
        _elements = new HashMap<>();

        for (T el : elements) {
            this.add(el);
        }
    }
    
    @Override
    public T add(T element) {

        _elements.put(element.id(), element);
        return element;
    }

    public void clear() {
    
        _elements.clear();
    }
    
    @Override
    public boolean contains(Integer id) {
    
        return _elements.containsKey(id);
    }
    
    @Override
    public T get(int objectId) {

        if (!this.contains(objectId)) {
            throw new IllegalArgumentException("Unknown object: " + objectId);
        } else {
            return _elements.get(objectId);
        }
    }
    
    @Override
    public T get(IdentifiableObject obj) {
	
	return this.get(obj.id());
    }
    
    @Override
    public boolean isEmpty() {

        return _elements.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {

        return _elements.values().iterator();
    }
    
    @Override
    public IDSet keys() {
        
        return new HashIDSet(_elements.keySet());
    }

    @Override
    public int length() {

        return _elements.size();
    }

    @Override
    public T put(T element) {

        return this.add(element);
    }

    @Override
    public T remove(int objectId) {

        return _elements.remove(objectId);
    }

    @Override
    public List<T> toList() {

        return new ArrayList<>(_elements.values());
    }
}
