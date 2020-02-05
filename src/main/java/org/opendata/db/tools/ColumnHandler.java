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
package org.opendata.db.tools;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import org.opendata.core.io.FileSystem;
import org.opendata.core.util.count.Counter;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnHandler {
    
    private final PrintWriter _out;
    private final HashMap<String, Counter> _terms;
    private final boolean _toUpper;

    public ColumnHandler(File file, boolean toUpper) throws java.io.IOException {

        _out = FileSystem.openPrintWriter(file);
        _terms = new HashMap<>();
        _toUpper = toUpper;
    }

    public ColumnHandler() {

        _out = null;
        _terms = null;
        _toUpper = false;
    }

    public void add(String value) {

        String term = value;
        if (_toUpper) {
            term = term.toUpperCase();
        }
        
        if (_out != null) {
            if (!_terms.containsKey(term)) {
                _terms.put(term, new Counter(1));
            } else {
                _terms.get(term).inc();
            }
        }
    }

    public void close() {

        if (_out != null) {
            for (String key : _terms.keySet()) {
                String term = key.replaceAll("\\t", " ").replaceAll("\\n", " ");
                _out.println(term + "\t" + _terms.get(key).value());
            }
            _out.close();
        }
    }
}
