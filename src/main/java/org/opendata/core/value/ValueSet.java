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
package org.opendata.core.value;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ValueSet implements Iterable<ValueCounter> {
    
    private final HashMap<String, ValueCounter> _values;
    
    public ValueSet() {
        
        _values = new HashMap<>();
    }
    
    public void add(String value) {
        
        if (_values.containsKey(value)) {
            _values.get(value).incCount();
        } else {
            _values.put(value, new ValueCounterImpl(value, 1));
        }
    }
    
    public void add(ValueCounter value) {
        
        _values.put(value.getText(), value);
    }
    
    public boolean isEmpty() {
        
        return _values.isEmpty();
    }

    @Override
    public Iterator<ValueCounter> iterator() {

        return _values.values().iterator();
    }
    
    public int size() {
        
        return _values.size();
    }
    
    public List<ValueCounter> values() {
        
        ArrayList<ValueCounter> values = new ArrayList<>(_values.values());
        Collections.sort(values, new Comparator<ValueCounter>() {
            @Override
            public int compare(ValueCounter v1, ValueCounter v2) {
                return v1.getText().compareTo(v2.getText());
            }
        });
        return values;
    }
    
    public int write(PrintWriter out) {
        
        int totalCount = 0;
        
        for (ValueCounter value : this.values()) {
            totalCount += value.getCount();
            out.println(value.getText() + "\t" + value.getCount());
        }
        
        return totalCount;
    }
}
