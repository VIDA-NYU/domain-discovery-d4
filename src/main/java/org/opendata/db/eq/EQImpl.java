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
package org.opendata.db.eq;

import org.opendata.core.object.IdentifiableObjectImpl;
import org.opendata.core.set.IDSet;

/**
 * An equivalence class is an identifiable set of terms. All terms in the
 * equivalence class always occur in the same set of columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQImpl extends IdentifiableObjectImpl implements EQ {
    
    private final IDSet _columns;
    private final IDSet _terms;
    private final int _termCount;
    
    public EQImpl(int id, IDSet terms, int termCount, IDSet columns) {
        
        super(id);
        
        _terms = terms;
        _termCount = termCount;
        _columns = columns;
    }
    
    public EQImpl(int id, IDSet terms, IDSet columns) {
        
        this(id, terms, terms.length(), columns);
    }
    
    @Override
    public IDSet columns() {
        
        return _columns;
    }
    
    @Override
    public IDSet terms() {
        
        return _terms;
    }
    
    @Override
    public int termCount() {
        
        return _termCount;
    }
}
