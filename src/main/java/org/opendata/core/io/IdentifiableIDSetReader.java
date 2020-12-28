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
import org.opendata.core.object.AnyObjectFilter;
import org.opendata.core.object.ObjectFilter;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;

/**
 * Read a set of identifiable ID sets from file in default file format.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IdentifiableIDSetReader extends IdentifiableIDSetFile {
    
    public IdentifiableObjectSet<IdentifiableIDSet> read(
            File file,
            ObjectFilter<Integer> filter,
            int listColumnIndex
    ) throws java.io.IOException {
        
        HashObjectSet<IdentifiableIDSet> result = new HashObjectSet<>();
        
        try (BufferedReader in = FileSystem.openReader(file)) {
            String line;
            while ((line = in.readLine()) != null) {
                IdentifiableIDSet set = this.parse(line, listColumnIndex);
                if (filter.contains(set.id())) {
                    result.add(set);
                }
            }
        }
        
        return result;
    }

    public IdentifiableObjectSet<IdentifiableIDSet> read(
            File file,
            ObjectFilter<Integer> filter
    ) throws java.io.IOException {
        
        return this.read(
                file,
                filter,
                IdentifiableIDSetFile.DEFAULT_LIST_COLUMN_INDEX
        );
    }
    
    public IdentifiableObjectSet<IdentifiableIDSet> read(File file, int listColumnIndex) throws java.io.IOException {
        
        return this.read(file, new AnyObjectFilter<>(), listColumnIndex);
    }

    public IdentifiableObjectSet<IdentifiableIDSet> read(File file) throws java.io.IOException {
        
        return this.read(
                file,
                new AnyObjectFilter<>(),
                IdentifiableIDSetFile.DEFAULT_LIST_COLUMN_INDEX
        );
    }
}
