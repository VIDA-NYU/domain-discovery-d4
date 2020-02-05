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
package org.opendata.core.io;

import java.io.BufferedReader;
import java.io.File;
import org.opendata.core.set.IdentifiableIDSet;

/**
 * Read a file containing identifiable ID sets in default format as a stream.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IdentifiableIDSetStreamReader extends IdentifiableIDSetFile implements AutoCloseable {

    private BufferedReader _in;
    private final int _listIndex;
    
    public IdentifiableIDSetStreamReader(BufferedReader in, int listIndex) {
        
        _in = in;
        _listIndex = listIndex;
    }
    
    public IdentifiableIDSetStreamReader(BufferedReader in) {
        
        this(in, IdentifiableIDSetFile.DEFAULT_LIST_COLUMN_INDEX);
    }
    
    public IdentifiableIDSetStreamReader(File file, int listIndex) throws java.io.IOException {
        
        this(FileSystem.openReader(file), listIndex);
    }
    
    public IdentifiableIDSetStreamReader(File file) throws java.io.IOException {
        
        this(file, IdentifiableIDSetFile.DEFAULT_LIST_COLUMN_INDEX);
    }
    
    @Override
    public void close() throws java.io.IOException {

        if (_in != null) {
            _in.close();
        }
    }
    
    public IdentifiableIDSet next() throws java.io.IOException {
        
        if (_in != null) {
            String line = _in.readLine();
            if (line != null) {
                return this.parse(line, _listIndex);
            } else {
                _in.close();
                _in = null;
            }
        }
        return null;
    }
}
