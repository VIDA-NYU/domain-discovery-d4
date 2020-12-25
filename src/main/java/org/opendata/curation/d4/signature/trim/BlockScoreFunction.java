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

import java.math.BigDecimal;
import java.util.HashMap;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.db.column.Column;

/**
 * Score function for signature blocks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class BlockScoreFunction {
    
    private final HashMap<Integer, int[]> _columns;
    private final HashMap<Integer, Integer> _columnSize;
    private final Integer[] _eqTermCounts;
    
    public BlockScoreFunction(
            Integer[] eqTermCounts,
            IdentifiableObjectSet<Column> columns
    ) {
        
        _eqTermCounts = eqTermCounts;
        
        _columns = new HashMap<>();
        _columnSize = new HashMap<>();
        
        for (IdentifiableIDSet column : columns) {
            _columns.put(column.id(), column.toArray());
            int size = 0;
            for (int nodeId : column) {
                size += _eqTermCounts[nodeId];
            }
            _columnSize.put(column.id(), size);
        }
        
    }
    
    /**
     * Get the maximum score of a signature block over all columns.
     * 
     * @param block
     * @param columns
     * @return 
     */
    public BigDecimal maxScore(int[] block, Iterable<Integer> columns) {
    
        BigDecimal max = BigDecimal.ZERO;
        
        for (int columnId : columns) {
            BigDecimal score = this.score(block, columnId);
            if (score.compareTo(max) > 0) {
                max = score;
            }
        }
        
        return max;
    }
    
    public abstract BigDecimal relevance(int columnSize, int blockSize, int overlap);
    
    /**
     * Return score of a signature block for a given column.
     * 
     * @param block
     * @param columnId
     * @return 
     */
    public BigDecimal score(int[] block, int columnId) {
        
        final int[] column = _columns.get(columnId);
        final int len1 = block.length;
        final int len2 = column.length;
        int idx1 = 0;
        int idx2 = 0;
        int blSize = 0;
        int overlap = 0;
        while ((idx1 < len1) && (idx2 < len2)) {
            final int nodeId = block[idx1];
            int comp = Integer.compare(nodeId, column[idx2]);
            if (comp < 0) {
                blSize += _eqTermCounts[nodeId];
                idx1++;
            } else if (comp > 0) {
                idx2++;
            } else {
                int nodeSize = _eqTermCounts[nodeId];
                blSize += nodeSize;
                overlap += nodeSize;
                idx1++;
                idx2++;
            }
        }
        if (overlap > 0) {
            while (idx1 < len1) {
                blSize += _eqTermCounts[block[idx1++]];
            }
            return this.relevance(_columnSize.get(columnId), blSize, overlap);
        } else {
            return BigDecimal.ZERO;
        }
    }
}
