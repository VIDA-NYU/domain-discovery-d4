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

import java.util.List;
import org.opendata.core.set.SortedIDList;

/**
 * Use two-dimensional array to represent signature blocks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class RobustSignatureImpl extends RobustSignature {

    private final SortedIDList[] _blocks;

    public RobustSignatureImpl(int id, List<SortedIDList> blocks) {
        
        super(id, blocks.size());
        
        _blocks = new SortedIDList[blocks.size()];
        for (int iBlock = 0; iBlock < blocks.size(); iBlock++) {
            _blocks[iBlock] = blocks.get(iBlock);
        }
    }
    
    public RobustSignatureImpl(int id, SortedIDList[] blocks) {
        
        super(id, blocks.length);
        
        _blocks = blocks;
    }

    @Override
    public SortedIDList get(int index) {

        return _blocks[index];
    }
}
