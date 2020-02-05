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

import java.io.File;
import java.io.PrintWriter;
import org.opendata.core.set.IdentifiableIDSet;

/**
 * Default writer for identifiable ID sets.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IdentifiableIDSetWriter extends IdentifiableIDSetFile implements AutoCloseable {
    
    private final PrintWriter _out;
    
    public IdentifiableIDSetWriter(File file) throws java.io.IOException {
        
        this(FileSystem.openPrintWriter(file));
    }
    
    public IdentifiableIDSetWriter(PrintWriter out) {
        
        _out = out;
    }
    
    @Override
    public void close() {

        _out.close();
    }

    public void write(IdentifiableIDSet value) {
        
        this.write(value, _out);
    }
}
