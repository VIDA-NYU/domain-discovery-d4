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
package org.opendata.core.set.similarity;

import java.util.ArrayList;
import java.util.List;
import org.opendata.core.set.IdentifiableIDSet;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DefaultIdentifiableIDSetEnumerator implements IdentifiableIDSetEnumerator {
    
    private final List<IdentifiableIDSet> _elements;

    public DefaultIdentifiableIDSetEnumerator(List<IdentifiableIDSet> elements) {
        
        _elements = elements;
    }
    
    @Override
    public Iterable<IdentifiableIDSet> getSet(int index) {

        ArrayList<IdentifiableIDSet> result = new ArrayList<>(_elements.size() - (index + 1));
        for (int iElement = index + 1; iElement < _elements.size(); iElement++) {
            result.add(_elements.get(iElement));
        }
        return result;
    }
}
