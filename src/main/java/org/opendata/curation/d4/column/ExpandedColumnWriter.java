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
package org.opendata.curation.d4.column;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;

/**
 * Expanded column consumer that writes all columns to a file on disk.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExpandedColumnWriter implements ExpandedColumnConsumer {

    private final File _file;
    private final HashMap<Integer, HashIDSet> _groups;
    private int _openCount = 0;
    private PrintWriter _out = null;
    
    public ExpandedColumnWriter(File file, HashMap<Integer, HashIDSet> groups) {
        
        _file = file;
        _groups = groups;
    }
    
    @Override
    public synchronized void close() {

        _openCount--;
        if (_openCount == 0) {
            _out.close();
            _out = null;
        }
    }

    @Override
    public synchronized void consume(ExpandedColumn column) {

        String line = column.originalNodes().toIntString();
        if (!column.expandedNodes().isEmpty()) {
            line += "\t" + column.expandedNodes().toIntString();
        }
        if (_groups.containsKey(column.id())) {
            for (int colId : _groups.get(column.id())) {
                _out.println(colId + "\t" + line);
            }
        } else {
            _out.println(column.id() + "\t" + line);
        }
    }

    @Override
    public synchronized void open() {

        if (_out == null) {
            try {
                _out = FileSystem.openPrintWriter(_file);
            } catch (java.io.IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        _openCount++;
    }
}
