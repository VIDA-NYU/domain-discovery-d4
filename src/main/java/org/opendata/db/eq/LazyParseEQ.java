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

import java.io.PrintWriter;
import org.opendata.core.object.IdentifiableObjectImpl;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.ImmutableIDSet;

/**
 * Alternative implementation for equivalence classes. Delays parsing the list
 * of columns and terms until their are first accessed..
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LazyParseEQ extends IdentifiableObjectImpl implements EQ {
    
    private final String _columns;
    private IDSet _columnsList = null;
    private final String _terms;
    private IDSet _termsList = null;
    
    public LazyParseEQ(int id, String terms, String columns) {
        
        super(id);
        
        _terms = terms;
        _columns = columns;
    }
    
    @Override
    public IDSet columns() {
        
        if (_columnsList == null) {
            _columnsList = EQImpl.parseColumnList(_columns);
        }
        return _columnsList;
    }
    
    @Override
    public IDSet terms() {
        
        if (_termsList == null) {
            _termsList = new ImmutableIDSet(_terms);
        }
        return _termsList;
    }
    
    @Override
    public void write(PrintWriter out) {
        
        out.println(String.format("%d\t%s\t%s", this.id(), _terms, _columns));
    }
}
