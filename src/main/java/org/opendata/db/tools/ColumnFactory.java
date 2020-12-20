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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.util.count.Counter;
import org.opendata.core.util.count.SimpleCounter;
import org.opendata.core.value.DefaultValueTransformer;

/**
 * Factory for column files. Writes column information to given output file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFactory {
   
    private static final Logger LOGGER = Logger
            .getLogger(ColumnFactory.class.getName());
    
    private final int _cacheSize;
    private final Counter _counter;
    private final PrintWriter _out;
    private final File _outputDir;
    
    public ColumnFactory(File outputDir, int cacheSize, PrintWriter out) {
        
        _outputDir = outputDir;
        _cacheSize = cacheSize;
        _out = out;

        _counter = new SimpleCounter();

        // Create output directory if it does not exist
        FileSystem.createFolder(outputDir);
    }
    
    public synchronized ColumnHandler getHandler(String dataset, String columnName) {

        int columnId = _counter.inc();
        String name = columnName.replaceAll("[^\\dA-Za-z]", "_");
        File outputFile = FileSystem.joinPath(
                _outputDir,
                columnId + "." + name + ".txt.gz"
        );
        try {
            ColumnHandler handler = new ColumnHandler(
                    outputFile,
                    new DefaultValueTransformer(),
                    _cacheSize
            );
            _out.println(columnId + "\t" + name + "\t" + dataset);
            return handler;
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, name, ex);
            return new ColumnHandler();
        }
    }
}
