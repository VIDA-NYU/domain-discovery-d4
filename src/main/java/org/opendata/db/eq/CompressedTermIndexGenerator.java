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
import java.util.HashMap;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.util.count.Counter;
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

    private final Counter _counter;
    private HashMap<String, MutableEQ> _eqIndex = null;
    private final PrintWriter _out;

    public CompressedTermIndexGenerator(PrintWriter out) {

        _out = out;

        _eqIndex = new HashMap<>();
        _counter = new Counter(0);
    }

    @Override
    public void close() {

        for (MutableEQ eq : _eqIndex.values()) {
            eq.write(_out);
        }

        System.out.println("NUMBER OF EQUIVALENCE CLASSES IS " + _eqIndex.size());
    }

    @Override
    public void consume(Term term) {

        String key = term.columns().toIntString();
        if (_eqIndex.containsKey(key)) {
            _eqIndex.get(key).add(term);
        } else {
            HashIDSet terms = new HashIDSet();
            terms.add(term.id());
            _eqIndex.put(
                    key,
                    new MutableEQ(_counter.inc(), term)
            );
        }
    }

    @Override
    public void open() {

    }
}
