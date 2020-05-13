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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.opendata.curation.d4.column.ExpandedColumn;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class HexEdgeReader implements ColumnEdgeReader {

    private final static short[][] BINARY = new short[][]{
        HexEdgeReader.toBinary(0),
        HexEdgeReader.toBinary(1),
        HexEdgeReader.toBinary(2),
        HexEdgeReader.toBinary(3),
        HexEdgeReader.toBinary(4),
        HexEdgeReader.toBinary(5),
        HexEdgeReader.toBinary(6),
        HexEdgeReader.toBinary(7),
        HexEdgeReader.toBinary(8),
        HexEdgeReader.toBinary(9),
        HexEdgeReader.toBinary(10),
        HexEdgeReader.toBinary(11),
        HexEdgeReader.toBinary(12),
        HexEdgeReader.toBinary(13),
        HexEdgeReader.toBinary(14),
        HexEdgeReader.toBinary(15),
    };
    
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
    public List<Integer> parseLine(String text) {

        ArrayList<Integer> edges = new ArrayList<>();
        int iBit = 0;
        for (char c : text.toCharArray()) {
            int index = -1;
            switch (c) {
                case '0':
                    index = 0;
                    break;
                case '1':
                    index = 1;
                    break;
                case '2':
                    index = 2;
                    break;
                case '3':
                    index = 3;
                    break;
                case '4':
                    index = 4;
                    break;
                case '5':
                    index = 5;
                    break;
                case '6':
                    index = 6;
                    break;
                case '7':
                    index = 7;
                    break;
                case '8':
                    index = 8;
                    break;
                case '9':
                    index = 9;
                    break;
                case 'A':
                    index = 10;
                    break;
                case 'B':
                    index = 11;
                    break;
                case 'C':
                    index = 12;
                    break;
                case 'D':
                    index = 13;
                    break;
                case 'E':
                    index = 14;
                    break;
                case 'F':
                    index = 15;
                    break;
                default:
                    throw new java.lang.IllegalArgumentException(Character.toString(c));
            }
            for (short b : BINARY[index]) {
                if (b == 1) {
                    edges.add(_mapping.get(iBit));
                }
                iBit++;
            }
        }
        return edges;
    }
    
    private static short[] toBinary(int decimal) {
        
        short[] result = new short[]{0, 0, 0, 0};
        
        String binary = Integer.toBinaryString(decimal);
        binary = StringUtils.leftPad(binary, 4, '0');
        int index = 0;
        for (char b : binary.toCharArray()) {
            if (b == '1') {
                result[index] = 1;
            }
            index++;
        }
        
        return result;
    }
}
