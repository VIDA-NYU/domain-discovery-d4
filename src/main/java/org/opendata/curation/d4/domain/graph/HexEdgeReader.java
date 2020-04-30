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

import java.util.HashMap;
import org.apache.commons.lang3.StringUtils;
import org.opendata.core.set.HashIDSet;
import org.opendata.curation.d4.column.ExpandedColumn;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class HexEdgeReader implements ColumnEdgeReader {

    private final ExpandedColumn _column;
    private final HashMap<Integer, Integer> _mapping;
            
    public HexEdgeReader(ExpandedColumn column) {
        
        _column = column;
        
        _mapping = new HashMap<>();
        for (int nodeId : _column.nodes().toArray()) {
            _mapping.put(_mapping.size(), nodeId);
        }
    }
    
    @Override
    public int[] parseLine(String text) {

        HashIDSet edges = new HashIDSet();
        int iBit = 0;
        for (char c : text.toCharArray()) {
            int decimal = Integer.parseInt(Character.toString(c), 16);
            String binary = Integer.toBinaryString(decimal);
            binary = StringUtils.leftPad(binary, 4, '0');
            for (char b : binary.toCharArray()) {
                if (b == '1') {
                    edges.add(_mapping.get(iBit));
                }
                iBit++;
            }
        }
        return edges.toArray();
    }
}
