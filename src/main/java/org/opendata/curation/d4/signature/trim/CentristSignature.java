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

import java.util.List;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.core.object.IdentifiableDouble;

/**
 * Trimmed signature is a wrapper around a signature blocks object. Instead of
 * creating a copy of the blocks only a list of block indexes is maintained that
 * references the non-pruned blocks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CentristSignature extends SignatureBlocks {

    private final List<IdentifiableDouble> _elements;
    private final SignatureBlocks _sig;
                    
    public CentristSignature(
            SignatureBlocks sig,
            List<IdentifiableDouble> elements,
            int dropIndex
    ) {
        super(sig.id(), sig.maxSim(), dropIndex);
        
        _sig = sig;
        _elements = elements;
    }
    
    @Override
    public int[] get(int index) {

        return _sig.get(_elements.get(index).id());
    }
}
