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
import java.util.HashMap;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.db.eq.CompressedTermIndex;
import org.opendata.db.eq.EQ;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermConsumer;
import org.opendata.db.term.TermIndexReader;

/**
 * Collection of methods that provide access to database terms.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Database {
    
    private class EQMappingReader implements TermConsumer {

        private final HashMap<Integer, EQTerms> _mapping;
        
        public EQMappingReader(HashMap<Integer, EQTerms> mapping) {
            
            _mapping = mapping;
        }
        
        @Override
        public void close() {

        }

        @Override
        public void consume(Term term) {

            EQTerms eqTerms = _mapping.get(term.id());
            if (eqTerms != null) {
                eqTerms.add(term.name());
            }
        }

        @Override
        public void open() {

        }
    }
    
    private class TermMappingReader implements TermConsumer {

        private final IDSet _filter;
        private final HashMap<Integer, String> _mapping;
        
        public TermMappingReader(IDSet filter) {
            
            _filter = filter;
            _mapping = new HashMap<>();
        }
        
        @Override
        public void close() {

        }

        @Override
        public void consume(Term term) {

            if (_filter.contains(term.id())) {
                _mapping.put(term.id(), term.name());
            }
        }

        public HashMap<Integer, String> mapping() {
            
            return _mapping;
        }
        
        @Override
        public void open() {

        }
    }
    
    private final CompressedTermIndex _eqIndex;
    private final TermIndexReader _termIndex;
    
    public Database(CompressedTermIndex eqIndex, TermIndexReader termIndex) {
        
        _eqIndex = eqIndex;
        _termIndex = termIndex;
    }
    
    private void addEq(HashMap<Integer, EQTerms> index, EQ eq, int sampleSize) {
        
        if (eq.termCount() == 1) {
            SingleTermEQ eqTems = new SingleTermEQ(eq.id());
            index.put(eq.terms()[0], eqTems);
        } else {
            Integer[] terms = eq.terms();
            MultiTermEQ eqTerms;
            if (terms.length > sampleSize) {
                eqTerms = new MultiTermEQ(eq.id(), terms.length, sampleSize);
                for (Integer termId : new HashIDSet(terms).sample(sampleSize)) {
                    index.put(termId, eqTerms);
                }
            } else {
                eqTerms = new MultiTermEQ(eq.id(), terms.length, terms.length);
                for (Integer termId : terms) {
                    index.put(termId, eqTerms);
                }
            }
        }
    }
    
    public HashMap<Integer, String> getTermIndex(IDSet filter) {
        
        TermMappingReader consumer = new TermMappingReader(filter);
        try {
            _termIndex.read(consumer);
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
        return consumer.mapping();
    }
        
    public HashMap<Integer, EQTerms> read(IDSet nodes, int sampleSize) {
        
        HashMap<Integer, EQTerms> termMapping = new HashMap<>();
        
        for (EQ eq : _eqIndex) {
            if (nodes.contains(eq.id())) {
                this.addEq(termMapping, eq, sampleSize);
            }
        }
        
        return this.readEQTerms(termMapping);
    }
    
    public HashMap<Integer, EQTerms> read(IDSet node) {
        
        return this.read(node, Integer.MAX_VALUE);
    }
    
    public HashMap<Integer, EQTerms> read(int columnId, int sampleSize) {
        
        HashMap<Integer, EQTerms> termMapping = new HashMap<>();
        
        for (EQ eq : _eqIndex) {
            if (Arrays.binarySearch(eq.columns(), columnId) >= 0) {
                this.addEq(termMapping, eq, sampleSize);
            }
        }
        
        return this.readEQTerms(termMapping);
    }
    
    public HashMap<Integer, EQTerms> read(int columnId) {
        
        return this.read(columnId, Integer.MAX_VALUE);
    }
    
    private HashMap<Integer, EQTerms> readEQTerms(HashMap<Integer, EQTerms> mapping) {
        
        try {
            _termIndex.read(new EQMappingReader(mapping));
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
        
        HashMap<Integer, EQTerms> result = new HashMap<>();
        for (EQTerms eq : mapping.values()) {
            if (!result.containsKey(eq.id())) {
                result.put(eq.id(), eq);
            }
        }
        return result;
    }
}
