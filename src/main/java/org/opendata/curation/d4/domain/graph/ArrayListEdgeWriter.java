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
package org.opendata.curation.d4.domain.graph;

import org.opendata.core.io.SynchronizedWriter;
import org.opendata.core.set.HashIDSet;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.signature.SignatureBlocks;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ArrayListEdgeWriter implements ColumnEdgeWriter{

    private final ExpandedColumn _column;
    private final SynchronizedWriter _out;

    public ArrayListEdgeWriter(ExpandedColumn column, SynchronizedWriter out) {

        _column = column;
        _out = out;
    }

    @Override
    public void close() {

    }

    @Override
    public void consume(SignatureBlocks sig) {

        if (_column.contains(sig.id())) {
            HashIDSet edges = new HashIDSet();
            for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
                for (int nodeId : sig.get(iBlock)) {
                    if (_column.contains(nodeId)) {
                        edges.add(nodeId);
                    }
                }
            }
            _out.write(_column.id() + "\t" + sig.id() + "\t" + edges.toIntString());
        }
    }

    @Override
    public void open() {

    }
}
