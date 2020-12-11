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
public class SignatureBlocksGenerator {
   
    private final MaxDropFinder<SignatureValue> _dropFinder;
    private final boolean _ignoreMinorDrop;

    public SignatureBlocksGenerator(
            MaxDropFinder<SignatureValue> dropFinder,
            boolean ignoreMinorDrop
    ) {
        
        _dropFinder = dropFinder;
        _ignoreMinorDrop = ignoreMinorDrop;
    }
    
    private SignatureBlock getBlock(List<SignatureValue> sig, int start, int end) {

        int[] block = new int[end - start];
        for (int iEl = start; iEl < end; iEl++) {
            block[iEl - start] = sig.get(iEl).id();
        }
        Arrays.sort(block);
        return new SignatureBlock(
                block,
                sig.get(start).value(),
                sig.get(end - 1).value()
        );
    }
    
    /**
     * Group elements in a context signature into blocks.Assumes that the
 elements in the context signature are sorted in decreasing order.
     * 
     * @param sig
     * @return 
     */
    public List<SignatureBlock> toBlocks(List<SignatureValue> sig) {
        
        ArrayList<SignatureBlock> blocks = new ArrayList<>();

        // No output if the context signautre is empty
        if (sig.isEmpty()) {
            return blocks;
        }
        
        int start = 0;
        final int end = sig.size();
        while (start < end) {
            Drop drop = _dropFinder.getSteepestDrop(sig, start);
            /*if (verbose) {
                System.out.println(
                        String.format(
                                "DROP @ %d (%s) WITH %f",
                                drop.index(),
                                Boolean.toString(drop.isFullSignature()),
                                drop.diff()
                        )
                );
            }*/
            int pruneIndex = drop.index();
            if (pruneIndex <= start) {
                break;
            } else if ((!drop.isFullSignature()) && (_ignoreMinorDrop)) {
                // If the ignoreMinorDrop flag is true check that the
                // difference at the drop is at least as large as the
                // difference between the elements in the block.
                double leftBound = sig.get(pruneIndex - 1).value();
                double blockDiff = sig.get(start).value() - leftBound;
                if (blockDiff > drop.diff()) {
                    // We encountered a minor drop. If the list of
                    // blocks is empty we ignore this minor drop to ensure that
                    // the first block is always included.
                    // Otherwise, we add the remaining elements as the
                    // final block.
                    if (!blocks.isEmpty()) {
                        pruneIndex = end;
                    }
                }
            }
            blocks.add(this.getBlock(sig, start, pruneIndex));
            start = pruneIndex;
        }
        
        return blocks;
    }
}
