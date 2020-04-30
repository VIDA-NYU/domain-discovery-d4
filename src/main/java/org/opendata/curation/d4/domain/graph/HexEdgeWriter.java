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
import org.opendata.core.io.SynchronizedWriter;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.signature.SignatureBlocks;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class HexEdgeWriter implements ColumnEdgeWriter{

    private final int _columnId;
    private final HashMap<Integer, Integer> _nodes;
    private final SynchronizedWriter _out;
    private final int _size;

    public HexEdgeWriter(ExpandedColumn column, SynchronizedWriter out) {

        _columnId = column.id();
        _out = out;

        _nodes = new HashMap<>();
        for (int nodeId : column.nodes().toArray()) {
            _nodes.put(nodeId, _nodes.size());
        }
        int mod = _nodes.size() % 4;
        if (mod != 0) {
            _size = _nodes.size() + (4 - mod);
        } else {
            _size = _nodes.size();
        }
    }

    public HexEdgeWriter(ExpandedColumn column) {
        
        this(column, null);
    }

    @Override
    public void close() {

    }

    @Override
    public void consume(SignatureBlocks sig) {

        if (_nodes.containsKey(sig.id())) {
            boolean[] edges = new boolean[_size];
            for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
                for (int nodeId : sig.get(iBlock)) {
                    Integer index = _nodes.get(nodeId);
                    if (index != null) {
                        edges[index] = true;
                    }
                }
            }
            String hexString = this.toHexString(edges);
            _out.write(_columnId + "\t" + sig.id() + "\t" + hexString);
        }
    }

    @Override
    public void open() {

    }
    
    public String toHexString(String text) {
        
        boolean[] edges = new boolean[_size];
        for (String token : text.split(",")) {
            int nodeId = Integer.parseInt(token);
            Integer index = _nodes.get(nodeId);
            if (index != null) {
                edges[index] = true;
            }
        }
        return this.toHexString(edges);
    }
    
    private String toHexString(boolean[] edges) {
        
        StringBuilder buf = new StringBuilder();
        int lastNonZero = 0;
        int iChar = 0;
        for (int iIndex = 0; iIndex < edges.length; iIndex += 4) {
            iChar++;
            StringBuilder bits = new StringBuilder();
            for (int iBit = iIndex; iBit < iIndex + 4; iBit++) {
                if (edges[iBit]) {
                    bits.append("1");
                } else {
                    bits.append("0");
                }
            }
            int decimal = Integer.parseInt(bits.toString(), 2);
            switch (decimal) {
                case 0:
                    buf.append(decimal);
                    break;
                case 10:
                    buf.append("A");
                    lastNonZero = iChar;
                    break;
                case 11:
                    buf.append("B");
                    lastNonZero = iChar;
                    break;
                case 12:
                    buf.append("C");
                    lastNonZero = iChar;
                    break;
                case 13:
                    buf.append("D");
                    lastNonZero = iChar;
                    break;
                case 14:
                    buf.append("E");
                    lastNonZero = iChar;
                    break;
                case 15:
                    buf.append("F");
                    lastNonZero = iChar;
                    break;
                default:
                    lastNonZero = iChar;
                    buf.append(decimal);
            }
        }
        String hexString = buf.toString();
        if (lastNonZero < hexString.length()) {
            hexString = hexString.substring(0, lastNonZero);
        }
        return hexString;
    }
}
