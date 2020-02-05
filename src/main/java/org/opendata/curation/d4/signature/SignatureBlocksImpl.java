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

import java.math.BigDecimal;
import java.util.List;

/**
 * Use two-dimensional array to represent signature blocks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksImpl extends SignatureBlocks {

    private final int[][] _blocks;

    public SignatureBlocksImpl(int id, BigDecimal maxSim, List<int[]> blocks) {
        
        super(id, maxSim, blocks.size());
        
        _blocks = new int[blocks.size()][];
        for (int iBlock = 0; iBlock < blocks.size(); iBlock++) {
            _blocks[iBlock] = blocks.get(iBlock);
        }
    }
    
    public SignatureBlocksImpl(int id, BigDecimal maxSim, int[][] blocks) {
        
        super(id, maxSim, blocks.length);
        
        _blocks = blocks;
    }

    @Override
    public int[] get(int index) {

        return _blocks[index];
    }
}
