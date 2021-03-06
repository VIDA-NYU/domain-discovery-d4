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
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.value.DefaultValueTransformer;

/**
 * Column factory that uses an index of columns that are included in the
 * output. Only for those columns that are contained in the given index
 * a active column handler will be returned. For all other columns a
 * dummy handler will be generated.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnIndexFactory implements ColumnFactory {

    private static final Logger LOGGER = Logger
            .getLogger(ColumnIndexFactory.class.getName());

    private final int _cacheSize;
    private final HashMap<String, HashSet<String>> _columns;
    private final File _outputDir;

    public ColumnIndexFactory(
            HashMap<String, HashSet<String>> columns,
            int cacheSize,
            File outputDir
    ) {
        _columns = columns;
        _outputDir = outputDir;
        _cacheSize = cacheSize;
    }

    @Override
    public ColumnHandler getHandler(String dataset, int columnIndex, String columnName) {

        HashSet<String> columns = _columns.get(dataset);
        if (columns.contains(columnName)) {
            String filename = String.format("%s.%d.%s.txt.gz", columnName.replaceAll("\\s+", "_").trim().replaceAll("/", "_"), columnIndex, dataset);
            columns.remove(columnName);
            try {
                return new ColumnHandler(
                    FileSystem.joinPath(_outputDir, filename),
                    new DefaultValueTransformer(),
                    _cacheSize
                );
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, filename, ex);
            }
        }
        return new ColumnHandler();
    }
}
