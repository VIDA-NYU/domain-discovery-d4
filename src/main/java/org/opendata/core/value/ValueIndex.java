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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Create an index of distinct values together with their frequency.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ValueIndex {
   
    private final HashMap<String, ValueCounter> _terms = new HashMap<>();
    
    public void add(String term) {
        
        if (term == null) {
            term = "";
        }
        
        if (!_terms.containsKey(term)) {
            _terms.put(term, new ValueCounterImpl(term, 1));
        } else {
            _terms.get(term).incCount();
        }
    }
    
    public List<ValueCounter> listAlphabetical(boolean reverse) {
        
        ArrayList<ValueCounter> values = new ArrayList<>(_terms.values());
        Collections.sort(values, (ValueCounter v1, ValueCounter v2) -> (
                v1.getText().compareTo(v2.getText())
        ));
        if (reverse) {
            Collections.reverse(values);
        }
        return values;
    }
    
    public List<ValueCounter> listAlphabetical() {
        
        return this.listAlphabetical(false);
    }
    
    public List<ValueCounter> listByFrequency(boolean reverse) {
        
        ArrayList<ValueCounter> values = new ArrayList<>(_terms.values());
        Collections.sort(values);
        if (reverse) {
            Collections.reverse(values);
        }
        return values;
    }
    
    public List<ValueCounter> listByFrequency() {
        
        return this.listByFrequency(false);
    }
}
