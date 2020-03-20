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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.opendata.core.util.StringHelper;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermConsumer;

/**
 * Compress a term index into a set of equivalence classes.
 * 
 * Each equivalence class has a unique identifier, a list of identifier for the
 * terms that belong to the equivalence class, and the list of identifier for
 * columns the equivalence class occurs in.
 * 
 * In some cases, the number of terms in the equivalence class can be huge.
 * To avoid having to load massive sets of term identifier the number of
 * identifier that are stored for each equivalence class can be limited by
 * the size limit parameter. In this case every equivalence class will maintain
 * at most the number of terms specified by the limit. The term count property
 * will still list the total number of terms that belong to the equivalence
 * class. Use the term count instead of the length of the term set as the
 * correct value for the size of an equivalence class.
 * 
 */
public class CompressedTermIndexGenerator implements TermConsumer {

    /**
     * Helper class that maintains a total count of terms belonging to a
     * equivalence class as well as the term identifier. The class maintains
     * term identifier only up to a given limit. All other terms are counted
     * but their identifier are not maintained.
     */
    private class TermIDSet {
        
        private final int _limit;
        private int _size;
        private final List<Integer> _terms;
        
        public TermIDSet(int termId, int limit) {
            
            _limit = limit;
            
            _terms = new ArrayList<>();
            _terms.add(termId);
            
            _size = 1;
        }
        
        public void add(int termId) {
            
            if (_terms.size() < _limit) {
                _terms.add(termId);
            }
            _size++;
        }
        
        public List<Integer> toSortedList() {
            
            Collections.sort(_terms);
            return _terms;
        }
        
        public int size() {
            
            return _size;
        }
    }
    
    private final String _domain;
    private HashMap<Integer, HashMap<String, TermIDSet>> _eqIndex = null;
    private final PrintWriter _out;
    private final int _sizeLimit;
    private int _termCount = 0;

    public CompressedTermIndexGenerator(PrintWriter out, String domain, int sizeLimit) {

        _out = out;
        _domain = domain;
        _sizeLimit = sizeLimit;
        
        _eqIndex = new HashMap<>();
    }

    public CompressedTermIndexGenerator(PrintWriter out) {
        
        this(out, null, Integer.MAX_VALUE);
    }
    
    @Override
    public void close() {

        int counter = 0;
        
        for (HashMap<String, TermIDSet> bucket : _eqIndex.values()) {
            for (String columns : bucket.keySet()) {
                TermIDSet terms = bucket.get(columns);
                _out.print(counter + "\t");
                _out.print(terms.size() + "\t");
                boolean isFirst = true;
                for (int termId : terms.toSortedList()) {
                    if (isFirst) {
                        _out.print(termId);
                        isFirst = false;
                    } else {
                        _out.print("," + termId);
                    }
                }
                _out.println("\t" + columns);
                counter++;
            }
        }

        if (_domain != null) {
            System.out.println(_domain + "\t" + _termCount + "\t" + counter);
        } else {
            System.out.println(_termCount + "\t" + counter);
        }
    }

    @Override
    public void consume(Term term) {

        List<Integer> values = term.columns().toSortedList();
        int index = values.get(0);
        String key =  StringHelper.joinIntegers(values);
        
        if (_eqIndex.containsKey(index)) {
            HashMap<String, TermIDSet> bucket = _eqIndex.get(index);
            if (bucket.containsKey(key)) {
                bucket.get(key).add(term.id());
            } else {
                bucket.put(key, new TermIDSet(term.id(), _sizeLimit));
            }
        } else {
            HashMap<String, TermIDSet> bucket = new HashMap<>();
            bucket.put(key, new TermIDSet(term.id(), _sizeLimit));
            _eqIndex.put(index, bucket);
        }
        _termCount++;
    }

    @Override
    public void open() {

        _termCount = 0;
    }
}
