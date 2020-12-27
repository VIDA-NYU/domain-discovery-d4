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
import java.util.List;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.curation.d4.signature.ContextSignatureBlock;
import org.opendata.curation.d4.signature.ContextSignatureBlocksConsumer;
import org.opendata.curation.d4.signature.ContextSignatureValue;

/**
 * Filter signature blocks based on column support. Includes only those blocks
 * the contain nodes that all occur together in at least one column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CommonColumnBlockFilter extends SignatureRobustifier {

    private final Integer[][] _columns;
    private final int _minStart;
    
    public CommonColumnBlockFilter(
            Integer[][] columns,
            int minStart,
            ContextSignatureBlocksConsumer consumer
    ) {
        super(consumer);
        
        _columns = columns;
        _minStart = minStart;
    }

    public CommonColumnBlockFilter(
            Integer[][] columns,
            ContextSignatureBlocksConsumer consumer
    ) {
        
        this(columns, 0, consumer);
    }

    @Override
    public void consume(int nodeId, BigDecimal sim, List<ContextSignatureBlock> blocks) {

        IDSet nodeColumns = new HashIDSet(_columns[nodeId]);
        
        int lastIndex = 0;
        for (ContextSignatureBlock block : blocks) {
            IDSet columns = nodeColumns;
            for (int iValue = 0; iValue < block.objectCount(); iValue++) {
                ContextSignatureValue member = block.objectAt(iValue);
                columns = columns.intersect(new HashIDSet(_columns[member.id()]));
                if (columns.isEmpty()) {
                    break;
                }
            }
            if (columns.isEmpty()) {
                break;
            }
            lastIndex++;
        }
        this.push(nodeId, sim, blocks, Math.max(_minStart, lastIndex));
    }
}
