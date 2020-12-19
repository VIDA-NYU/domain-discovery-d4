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
package org.opendata.core.graph;

import org.opendata.core.object.IdentifiableObjectImpl;

/**
 * Node in a graph that has a unique identifier and a list of associated
 * elements. The semantic of the element identifier is not further defined.
 * The intention for these nodes is to support fast computation of overlap
 * between the element sets of two nodes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Node extends IdentifiableObjectImpl {

    private final int[] _elements;

    public Node(int id, int[] elements) {

        super(id);

        _elements = elements;
    }

    public int elementCount() {

        return _elements.length;
    }

    public int[] elements() {

        return _elements;
    }

    public int overlap(Node node) {

        final int[] colJ = node.elements();
        final int lenI = _elements.length;
        final int lenJ = colJ.length;
        int idxI = 0;
        int idxJ = 0;

        int overlap = 0;
        while ((idxI < lenI) && (idxJ < lenJ)) {
            if (_elements[idxI] < colJ[idxJ]) {
                idxI++;
            } else if (_elements[idxI] > colJ[idxJ]) {
                idxJ++;
            } else {
                overlap++;
                idxI++;
                idxJ++;
            }
        }
        return overlap;
    }
}
