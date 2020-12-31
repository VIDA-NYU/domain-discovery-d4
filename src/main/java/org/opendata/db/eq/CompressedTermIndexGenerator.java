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
import java.util.HashMap;
import java.util.List;
import org.opendata.core.object.IdentifiableInteger;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.SortedObjectSet;
import org.opendata.core.util.IdentifiableCount;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermConsumer;

/**
 * Compress a term index into a set of equivalence classes.
 * 
 * The observeFrequencies flag determines if equivalence classes will
 * contain terms that always occur together in the same columns without
 * considering their frequency of occurrence or whether frequencies are
 * taken into account, i.e., the terms occur in the same columns always with
 * the same frequency.
 * 
 */
public class CompressedTermIndexGenerator implements TermConsumer {

    private class MutableEQ  {

        private final SortedObjectSet<IdentifiableCount> _columns;
        private final List<Integer> _terms;

        public MutableEQ(Term term) {

            _terms = new ArrayList<>();
            _terms.add(term.id());

            SortedObjectSet<IdentifiableInteger> columns = term.columns();
            IdentifiableCount[] counts = new IdentifiableCount[columns.objectCount()];
            for (int iEl = 0; iEl < columns.objectCount(); iEl++) {
                IdentifiableInteger col = columns.objectAt(iEl);
                counts[iEl] = new IdentifiableCount(col.id(), col.value());
            }
            _columns = new SortedObjectSet<>(counts);
        }
        
        /**
         * Add a term to the equivalence class. This assumes that the list of
         * columns for the added term is the same as the list of columns for all
         * other terms in the equivalence class.
         * 
         * @param term 
         */
        public void add(Term term) {

            _terms.add(term.id());
            SortedObjectSet<IdentifiableInteger> columns = term.columns();
            for (int iCol = 0; iCol < _columns.objectCount(); iCol++) {
                IdentifiableCount counter = _columns.objectAt(iCol);
                IdentifiableInteger col = columns.objectAt(iCol);
                if (counter.id() != col.id()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Mismatch at position %d: %d <> %d",
                                    iCol,
                                    counter.id(),
                                    col.id()
                            )
                    );
                }
                counter.inc(col.value());
            }
        }

        public SortedObjectSet<IdentifiableCount> columns() {
            
            return _columns;
        }
        
        public List<Integer> terms() {
        
            return _terms;
        }
    }

    private HashMap<String, MutableEQ> _eqIndex = null;
    private final boolean _verbose;
    private final EQWriter _writer;

    public CompressedTermIndexGenerator(EQWriter writer, boolean verbose) {

        _writer = writer;

        _eqIndex = new HashMap<>();
        _verbose = verbose;
    }

    public CompressedTermIndexGenerator(PrintWriter out, boolean verbose) {

        _writer = new EQFileWriter(out);

        _eqIndex = new HashMap<>();
        _verbose = verbose;
    }

    @Override
    public void close() {

        for (MutableEQ eq : _eqIndex.values()) {
            _writer.write(eq.terms(), eq.columns());
        }
        
        if (_verbose) {
            System.out.println("NUMBER OF EQUIVALENCE CLASSES IS " + _eqIndex.size());
        }
    }

    @Override
    public void consume(Term term) {

        SortedObjectSet<IdentifiableInteger> columns = term.columns();
        String key = columns.key();
        if (_eqIndex.containsKey(key)) {
            _eqIndex.get(key).add(term);
        } else {
            HashIDSet terms = new HashIDSet();
            terms.add(term.id());
            _eqIndex.put(key, new MutableEQ(term));
        }
    }

    @Override
    public void open() {

    }
}
