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
package org.opendata.db.term;

import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.set.StringSet;

/**
 * Find matching terms for a given set of values.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TermFinder implements TermConsumer {

    private HashObjectSet<Term> _terms = null;
    private final StringSet _values;
    
    public TermFinder(StringSet values) {
        
        _values = values;
    }
    
    @Override
    public void close() {

    }

    @Override
    public void consume(Term term) {

        if (_values.contains(term.name())) {
            _terms.add(term);
        }
    }

    @Override
    public void open() {

        _terms = new HashObjectSet<>();
    }
    
    public IdentifiableObjectSet<Term> terms() {
        
        return _terms;
    }
}
