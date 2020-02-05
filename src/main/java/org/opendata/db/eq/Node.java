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

import org.opendata.core.object.IdentifiableObjectImpl;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Node extends IdentifiableObjectImpl {

    private final int[] _columns;
    private final int _termCount;

    public Node(int id, int[] columns, int termCount) {

        super(id);

        _columns = columns;
        _termCount = termCount;
    }

    public Node(EQ eq) {
    
        this(eq.id(), eq.columns().toArray(), eq.terms().length());
    }
    
    public int columnCount() {

        return _columns.length;
    }

    public int[] columns() {

        return _columns;
    }

    public int overlap(Node node) {

        final int[] colJ = node.columns();
        final int lenI = _columns.length;
        final int lenJ = colJ.length;
        int idxI = 0;
        int idxJ = 0;

        int overlap = 0;
        while ((idxI < lenI) && (idxJ < lenJ)) {
            if (_columns[idxI] < colJ[idxJ]) {
                idxI++;
            } else if (_columns[idxI] > colJ[idxJ]) {
                idxJ++;
            } else {
                overlap++;
                idxI++;
                idxJ++;
            }
        }
        return overlap;
    }
    
    public int termCount() {
        
        return _termCount;
    }
}
