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

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import org.opendata.core.io.FileSystem;
import org.opendata.core.util.count.Counter;
import org.opendata.core.util.count.SimpleCounter;
import org.opendata.core.value.ValueTransformer;

/**
 * The column handler is responsible for creating a unique set of column values
 * from a stream of values. The handler maintains the frequency for each unique
 * term. Terms are modified using the given value transformer.
 * 
 * The resulting file is tab-delimited with two columns: the term and the term
 * frequency.
 * 
 * This implementation maintains a cache of terms in memory. If the cache size
 * is exceeded terms are evicted form the cache to the output file. At then end
 * of the stream any term that has been written to file will be merged with the
 * terms that are left in the cache and the result is written to the output
 * file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnHandler {
    
    private final LinkedList<String> _cache;
    private final int _cacheSize;
    private final File _file;
    private boolean _hasOutput;
    private PrintWriter _out;
    private final HashMap<String, Counter> _terms;
    private final ValueTransformer _transformer;

    public ColumnHandler(
            File file,
            ValueTransformer transformer,
            int cacheSize
    ) throws java.io.IOException {

        _file = file;
        _terms = new HashMap<>();
        _transformer = transformer;
        _cacheSize = cacheSize;
        
        _out = FileSystem.openPrintWriter(file);
        _cache = new LinkedList<>();
        // Maintain information whether elements were evicted from the cache
        // and written to file.
        _hasOutput = false;
    }

    /**
     * Create a dummy column handler for column files that cannot be read.
     * 
     */
    public ColumnHandler() {

        _file = null;
        _out = null;
        _terms = null;
        _transformer = null;
        _cacheSize = -1;
        
        _cache = null;
        _hasOutput = false;
    }
    
    public void add(String value) {

        if (_out != null) {
            String term = _transformer.transform(value);
            if (!term.equals("")) {
                if (!_terms.containsKey(term)) {
                    _terms.put(term, new SimpleCounter(1));
                    _cache.offer(term);
                    if (_cache.size() > _cacheSize) {
                        // Evict a term from the cache if the maximum cache
                        // size is reached. The term and its current count is
                        // written to the output file.
                        String evict = _cache.poll();
                        this.write(evict, _terms.remove(evict));
                        _hasOutput = true;
                    }
                } else {
                    _terms.get(term).inc();
                }
            }
        }
    }

    public void close() {

        if (_out != null) {
            // Read evicted terms (if any).
            if (_hasOutput) {
                _out.close();
                try (BufferedReader in = FileSystem.openReader(_file)) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        String[] tokens = line.split("\t");
                        String term = tokens[0];
                        int count = Integer.parseInt(tokens[1]);
                        if (!_terms.containsKey(term)) {
                            _terms.put(term, new SimpleCounter(count));
                        } else {
                            _terms.get(term).inc(count);
                        }
                    }
                } catch (java.io.IOException ex) {
                    throw new RuntimeException(ex);
                }
                try {
                    _out = FileSystem.openPrintWriter(_file);
                } catch (java.io.IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
            for (String term : _terms.keySet()) {
                this.write(term, _terms.get(term));
            }
            _out.close();
        }
    }
    
    private void write(String term, Counter count) {
        
        _out.println(String.format("%s\t%d", term, count.value()));
    }
}
