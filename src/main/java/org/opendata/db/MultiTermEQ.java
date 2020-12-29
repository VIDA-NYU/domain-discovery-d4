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
package org.opendata.db;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Arrays;
import java.util.Iterator;
import org.opendata.core.object.IdentifiableObjectImpl;

/**
 * Term set for an equivalence class with multiple terms.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class MultiTermEQ extends IdentifiableObjectImpl implements EQTerms {

    private final int _eqTermCount;
    private int _index;
    private final String[] _terms;
    
    public MultiTermEQ(int id, int eqTermCount, int size) {
        
        super(id);
        
        _eqTermCount = eqTermCount;
        _terms = new String[size];
        
        _index = 0;
    }
    
    @Override
    public void add(String term) {

        _terms[_index++] = term;
    }

    @Override
    public String get(int index) {

        if (_index == _terms.length) {
            return _terms[index];
        }
        throw new RuntimeException("Set is incomplete");
    }

    @Override
    public Iterator<String> iterator() {

        if (_index == _terms.length) {
            return Arrays.asList(_terms).iterator();
        }
        throw new RuntimeException("Set is incomplete");
    }

    @Override
    public int size() {

        return _terms.length;
    }    

    @Override
    public int termCount() {

        return _eqTermCount;
    }

    @Override
    public JsonObject toJsonObject() {

        JsonObject doc = new JsonObject();
        doc.add("id", new JsonPrimitive(this.id()));
        JsonArray terms = new JsonArray();
        for (String term : _terms) {
            terms.add(new JsonPrimitive(term));
        }
        doc.add("terms", terms);
        return doc;
    }
}
