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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import org.opendata.core.io.FileSetReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.object.AnyObjectFilter;
import org.opendata.core.object.ObjectFilter;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExpandedColumnReader extends FileSetReader implements ExpandedColumnStream {

    public ExpandedColumnReader(File file) {
        
        super(file, false);
    }
    
    public IdentifiableObjectSet<ExpandedColumn> read() throws java.io.IOException {
        
        ExpandedColumnBuffer buffer = new ExpandedColumnBuffer();
        this.stream(buffer);
        return new HashObjectSet<>(buffer.columns());
    }
    
    @Override
    public void stream(ExpandedColumnConsumer consumer) throws java.io.IOException {

        this.stream(consumer, new AnyObjectFilter());
    }

    @Override
    public void stream(ExpandedColumnConsumer consumer, ObjectFilter<Integer> filter) throws IOException {

        consumer.open();

        for (File file : this) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    int id = Integer.parseInt(tokens[0]);
                    if (filter.contains(id)) {
                        IDSet nodes = new HashIDSet(tokens[1].split(","));
                        IDSet expansion;
                        if (tokens.length >= 3) {
                            expansion = new HashIDSet(tokens[2].split(","));
                        } else {
                            expansion = new HashIDSet();
                        }
                        ImmutableExpandedColumn column;
                        column = new ImmutableExpandedColumn(id, nodes, expansion);
                        consumer.consume(column);
                    }
                }
            }
        }

        consumer.close();
    }
}
