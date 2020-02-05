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
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.ImmutableIDSet;

/**
 * An equivalence class is an identifiable set of terms. All terms in the
 * equivalence class always occur in the same set of columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQImpl extends IdentifiableObjectImpl implements EQ {
    
    private final IDSet _columns;
    private final IDSet _terms;
    
    public EQImpl(int id, IDSet terms, IDSet columns) {
        
        super(id);
        
        _terms = terms;
        _columns = columns;
    }

    public EQImpl(String[] tokens) {

        this(
                Integer.parseInt(tokens[0]),
                new ImmutableIDSet(tokens[1]),
                parseColumnList(tokens[2])
        );
    }
    
    @Override
    public IDSet columns() {
        
        return _columns;
    }
    
    private static IDSet parseColumnList(String list) {
    
        HashIDSet columns = new HashIDSet();
        
        for (String token : list.split(",")) {
            if (token.contains(":")) {
                columns.add(Integer.parseInt(token.substring(0, token.indexOf(":"))));
            } else {
                columns.add(Integer.parseInt(token));
            }
        }
        return columns;
    }
    
    @Override
    public IDSet terms() {
        
        return _terms;
    }
    
    @Override
    public void write(PrintWriter out) {
        
        out.println(
                this.id() + "\t" +
                this.terms().toIntString() + "\t" +
                this.columns().toIntString()
        );
    }
}
