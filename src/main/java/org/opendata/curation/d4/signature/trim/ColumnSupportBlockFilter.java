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
package org.opendata.curation.d4.signature.trim;

import java.util.HashMap;
import java.util.List;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureBlock;
import org.opendata.core.set.SortedIDList;

/**
 * Filter signature blocks based on column support. Includes only those blocks
 * the contain nodes that all occur together in at least one column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnSupportBlockFilter extends SignatureRobustifier {

    private final HashMap<Integer, SortedIDList> _eqIndex;
    private final int _minStart;
    
    public ColumnSupportBlockFilter(
            HashMap<Integer, SortedIDList> eqIndex,
            int minStart,
            SignatureBlocksConsumer consumer
    ) {
        super(consumer);
        
        _eqIndex = eqIndex;
        _minStart = minStart;
    }

    public ColumnSupportBlockFilter(
            HashMap<Integer, SortedIDList> eqIndex,
            SignatureBlocksConsumer consumer
    ) {
        
        this(eqIndex, 0, consumer);
    }

    @Override
    public void consume(int nodeId, List<SignatureBlock> blocks) {

        SortedIDList nodeColumns = _eqIndex.get(nodeId);
        
        int lastIndex = 0;
        for (SignatureBlock block : blocks) {
            for (int memberId : block) {
                nodeColumns = nodeColumns.intersect(_eqIndex.get(memberId));
                if (nodeColumns.isEmpty()) {
                    break;
                }
            }
            if (nodeColumns.isEmpty()) {
                break;
            }
            lastIndex++;
        }
        this.push(nodeId, blocks, Math.max(_minStart, lastIndex));
    }
}
