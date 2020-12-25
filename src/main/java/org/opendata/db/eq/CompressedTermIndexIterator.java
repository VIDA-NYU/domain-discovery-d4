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

import java.io.BufferedReader;
import java.io.File;
import java.util.Iterator;
import org.opendata.core.io.FileSystem;

/**
 * Iterator for a compressed term index file.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CompressedTermIndexIterator implements Iterator<EQ> {

    private boolean _autoclose;
    private final BufferedReader _in;
    private EQ _next = null;
    
    public CompressedTermIndexIterator(BufferedReader in, boolean autoclose) throws java.io.IOException {
        
        _in = in;
        _autoclose = autoclose;
        
        this.next();
    }
    
    public CompressedTermIndexIterator(File eqFile) throws java.io.IOException {
        
        this(FileSystem.openReader(eqFile), true);
    }
    
    @Override
    public boolean hasNext() {

        return (_next != null);
    }

    @Override
    public final EQ next() {

        EQ result = _next;
        
        try {
            String line = _in.readLine();
            if (line != null) {
                _next = new EQImpl(line);
            } else {
                if (_autoclose) {
                    _in.close();
                }
                _next = null;
            }
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
        
        return result;
    }
}
