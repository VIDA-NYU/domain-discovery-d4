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

import java.util.Arrays;
import java.util.Iterator;
import org.opendata.core.object.IdentifiableObjectImpl;

/**
 * term set for an equivalence class with a single term.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SingleTermEQ extends IdentifiableObjectImpl implements EQTerms {

    private String _term = null;
    
    public SingleTermEQ(int id) {
        
        super(id);
    }

    @Override
    public void add(String term) {

        if (_term == null) {
            _term = term;
        } else {
            throw new RuntimeException("Set already complete");
        }
    }
    
    @Override
    public int eqTermCount() {

        return 1;
    }

    @Override
    public String get(int index) {

        if ((index == 0) && (_term != null)) {
            return _term;
        }
        throw new ArrayIndexOutOfBoundsException(index);
    }

    @Override
    public Iterator<String> iterator() {

        if (_term != null) {
            return Arrays.asList(new String[]{_term}).iterator();
        }
        throw new RuntimeException("Set is incomplete");
    }

    @Override
    public int size() {

        return 1;
    }    
}
