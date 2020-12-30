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

import org.opendata.curation.d4.signature.MultiBlockSignature;
import java.util.ArrayList;
import java.util.List;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.SortedIDArray;
import org.opendata.core.set.SortedIDList;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;
import org.opendata.curation.d4.signature.SignatureBlock;

/**
 * Overlap signature blocks trimmer. Maintains all blocks that overlap with the
 * column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class OverlapTrimmer extends SignatureTrimmer {

    private final SortedIDList _column;
    
    public OverlapTrimmer(
            IDSet column,
            RobustSignatureConsumer consumer
    ) {
        super(column, consumer);
        
        _column = new SortedIDArray(column.toSortedList());
    }

    private boolean overlap(SignatureBlock block, SortedIDList column) {
        
        final int len1 = block.elementCount();
        final int len2 = column.elementCount();
        
        int idx1 = 0;
        int idx2 = 0;
        
        while ((idx1 < len1) && (idx2 < len2)) {
            int comp = Integer.compare(block.elementAt(idx1), column.elementAt(idx2));
            if (comp < 0) {
                idx1++;
            } else if (comp > 0) {
                idx2++;
            } else {
                return true;
            }
        }
        return false;
    }

    @Override
    public void trim(int id, List<SignatureBlock> blocks, RobustSignatureConsumer consumer) {

        List<SignatureBlock> sig = new ArrayList<>();
        for (SignatureBlock block : blocks) {
            if (this.overlap(block, _column)) {
                sig.add(block);
            }
        }
        if (!sig.isEmpty()) {
            consumer.consume(new MultiBlockSignature(id, sig));
        }
    }
}
