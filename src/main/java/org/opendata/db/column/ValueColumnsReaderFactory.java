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
package org.opendata.db.column;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import org.opendata.core.object.filter.AnyObjectFilter;
import org.opendata.core.object.filter.ObjectFilter;
import org.opendata.core.value.ValueCounter;

/**
 * Reader factory for column files. Uses the flexible column reader.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ValueColumnsReaderFactory implements ColumnReaderFactory<ValueCounter> {

    private final LinkedList<File> _files;
    private final int _hashLengthThreshold;
    
    public ValueColumnsReaderFactory(
            File directory,
            ObjectFilter<Integer> filter,
            int hashLengthThreshold
    ) {
        if (!directory.exists()) {
            throw new IllegalArgumentException("Directory " + directory.getAbsolutePath() + " does not exist");
        } else if (!directory.isDirectory()) {
            throw new IllegalArgumentException(directory.getAbsolutePath() + " not a directory");
        }
        
        _hashLengthThreshold = hashLengthThreshold;

        _files = new LinkedList<>();

        for (File file : directory.listFiles()) {
            if ((file.getName().endsWith(".txt")) || (file.getName().endsWith(".txt.gz"))) {
                    int columnId = ColumnHelper.getColumnId(file);
                    if (filter.contains(columnId)) {
                        _files.add(file);
                    }
            }
        }
    }
    
    public ValueColumnsReaderFactory(File directory, int hashLengthThreshold) {
	
        this(directory, new AnyObjectFilter<Integer>(), hashLengthThreshold);
    }
    
    public ValueColumnsReaderFactory(List<File> files, int hashLengthThreshold) {
        
        _files = new LinkedList<>(files);
        _hashLengthThreshold = hashLengthThreshold;
    }
    
    @Override
    public boolean hasNext() {

        return (!_files.isEmpty());
    }

    @Override
    public ColumnReader<ValueCounter> next() {

        File file = _files.pop();
        int columnId = ColumnHelper.getColumnId(file);
        return new FlexibleColumnReader(file, columnId, _hashLengthThreshold);
    }
}
