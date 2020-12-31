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

import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import org.opendata.core.object.IdentifiableInteger;
import org.opendata.core.set.SortedObjectSet;
import org.opendata.core.set.SortedObjectSetIterator;
import org.opendata.core.util.StringHelper;
import org.opendata.db.column.ColumnHelper;

/**
 * Writer for compressed term index files.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQFileWriter implements EQWriter {

    private int _counter;
    private final PrintWriter _out;

    public EQFileWriter(PrintWriter out) {

        _out = out;
        _counter = 0;
    }

    @Override
    public <T extends IdentifiableInteger> void write(List<Integer> terms, SortedObjectSet<T> columns) {

        Collections.sort(terms);

        _out.println(
                String.format(
                        "%d\t%s\t%s",
                        _counter++,
                        StringHelper.joinIntegers(terms),
                        ColumnHelper.toArrayString(new SortedObjectSetIterator<>(columns))
                )
        );
    }
}

