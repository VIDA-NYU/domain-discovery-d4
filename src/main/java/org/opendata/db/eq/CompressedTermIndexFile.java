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

import java.io.File;
import java.util.Iterator;

/**
 * Collection of equivalence classes that are maintained in a file on disk.
 * Reads the list of identifier and only keeps them in memory.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CompressedTermIndexFile implements CompressedTermIndex {
   
    private final File _file;
    
    public CompressedTermIndexFile(File file) {
        
        _file = file;
    }

    @Override
    public Iterator<EQ> iterator() {

        try {
            return new CompressedTermIndexIterator(_file);
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
