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
package org.opendata.curation.d4.signature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.opendata.core.prune.Drop;
import org.opendata.core.prune.MaxDropFinder;

/**
 * Generator that groups elements in a context signature into blocks based on
 * steepest drop.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ContextSignatureProcessor {
   
    private final MaxDropFinder<ContextSignatureValue> _dropFinder;
    private final Integer[] _eqTermCounts;
    private final boolean _ignoreMinorDrop;
    private final boolean _includeBlockBeforeMinor;

    public ContextSignatureProcessor(
            Integer[] eqTermCounts,
            MaxDropFinder<ContextSignatureValue> dropFinder,
            boolean ignoreMinorDrop,
            boolean includeBlockBeforeMinor
    ) {
        _eqTermCounts = eqTermCounts;
        _dropFinder = dropFinder;
        _ignoreMinorDrop = ignoreMinorDrop;
        _includeBlockBeforeMinor = includeBlockBeforeMinor;
    }
    
    private ContextSignatureBlock getBlock(List<ContextSignatureValue> sig, int start, int end) {

        ContextSignatureValue[] block = new ContextSignatureValue[end - start];
        int termCount = 0;
        for (int iEl = start; iEl < end; iEl++) {
            ContextSignatureValue el = sig.get(iEl);
            block[iEl - start] = el;
            termCount += _eqTermCounts[el.id()];
        }
        Arrays.sort(block);
        return new ContextSignatureBlock( block, termCount);
    }
    
    /**
     * Group elements in a context signature into blocks.
     * 
     * @param sig
     * @param consumer
     */
    public void process(ContextSignature sig, ContextSignatureBlocksConsumer consumer) {
        
        ArrayList<ContextSignatureBlock> blocks = new ArrayList<>();

        // No output if the context signautre is empty
        if (sig.isEmpty()) {
            return;
        }
        
        List<ContextSignatureValue> elements = sig.rankedElements();
        int start = 0;
        final int end = sig.size();
        while (start < end) {
            Drop drop = _dropFinder.getSteepestDrop(elements, start);
            int pruneIndex = drop.index();
            boolean isMinorDrop = false;
            if (pruneIndex <= start) {
                break;
            } else if ((!drop.isFullSignature()) && (_ignoreMinorDrop)) {
                // If the ignoreMinorDrop flag is true check that the
                // difference at the drop is at least as large as the
                // difference between the elements in the block.
                double leftBound = elements.get(pruneIndex - 1).value();
                double blockDiff = elements.get(start).value() - leftBound;
                if (blockDiff > drop.diff()) {
                    // We encountered a minor drop. If the list of
                    // blocks is empty we ignore this minor drop to ensure that
                    // the first block is always included.
                    // Otherwise, we add the remaining elements as the
                    // final block.
                    if (!blocks.isEmpty()) {
                        // Depending on the value of the includeBlockBeforeMinor
                        // flag we either include all remaining elements in the
                        // filal block (false) or include the block until the
                        // current prune index and then the remaining elements
                        // as a final block (true);
                        if (_includeBlockBeforeMinor) {
                            // Ensure that the block until the current prune
                            // index is inclded as separate block.
                            isMinorDrop = true;
                        } else {
                            // Put all elements since the previous drop into
                            // one final block.
                            pruneIndex = end;
                        }
                    }
                }
            }
            blocks.add(this.getBlock(elements, start, pruneIndex));
            if (isMinorDrop) {
                if (pruneIndex < end) {
                    start = pruneIndex;
                    pruneIndex = end;
                    blocks.add(this.getBlock(elements, start, pruneIndex));
                }
            }
            start = pruneIndex;
        }
        if (!blocks.isEmpty()) {
            consumer.consume(sig.id(), elements.get(0).toBigDecimal(), blocks);
        }
    }
}
