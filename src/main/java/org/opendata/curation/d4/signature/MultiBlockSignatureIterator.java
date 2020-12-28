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

import java.util.Iterator;
import java.util.List;

/**
 * Iterator for robust signatures that contain multiple signature blocks.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class MultiBlockSignatureIterator implements Iterator<Integer> {

    private final List<SignatureBlock> _blocks;
    private int _blockIndex;
    private Iterator<Integer> _iterator;
    
    public MultiBlockSignatureIterator(List<SignatureBlock> blocks) {
        
        _blocks = blocks;
        
        if (blocks.isEmpty()) {
            _iterator = null;
            _blockIndex = 0;
        } else {
            _iterator = blocks.get(0).iterator();
            _blockIndex = 1;
        }
    }
    
    @Override
    public boolean hasNext() {

        if (_iterator != null) {
            if (_iterator.hasNext()) {
                return true;
            }
            while (_blockIndex < _blocks.size()) {
                _iterator = _blocks.get(_blockIndex++).iterator();
                if (_iterator.hasNext()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Integer next() {
        
        return _iterator.next();
    }    
}
