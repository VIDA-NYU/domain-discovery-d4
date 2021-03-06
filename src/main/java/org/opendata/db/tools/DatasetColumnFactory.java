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
import org.opendata.core.value.DefaultValueTransformer;

/**
 * Column file factory when processing columns from datasets of different
 * domains. Column files are named by dataset identifier and column index
 * instead of the column name.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatasetColumnFactory implements ColumnFactory {
   
    private static final Logger LOGGER = Logger
            .getLogger(DatasetColumnFactory.class.getName());
    
    private final int _cacheSize;
    private final PrintWriter _out;
    private final File _outputDir;
    
    public DatasetColumnFactory(File outputDir, int cacheSize, PrintWriter out) {
        
        _outputDir = outputDir;
        _cacheSize = cacheSize;
        _out = out;
    }
    
    @Override
    public synchronized ColumnHandler getHandler(String dataset, int columnIndex, String columnName) {

        File outputFile = FileSystem.joinPath(
                _outputDir,
                dataset + "." + columnIndex + ".txt.gz"
        );
        try {
            ColumnHandler handler = new ColumnHandler(
                    outputFile,
                    new DefaultValueTransformer(),
                    _cacheSize
            );
            _out.println(
                    String.format(
                            "%s\t%d\t%s",
                            dataset,
                            columnIndex,
                            columnName.replaceAll("\\s+", " ")
                    )
            );
            return handler;
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, String.format("%s (%d)", dataset, columnIndex), ex);
            return new ColumnHandler();
        }
    }
}
