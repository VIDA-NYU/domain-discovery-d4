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
package org.opendata.curation.d4.domain;

import java.math.BigDecimal;
import org.opendata.core.object.IdentifiableObjectImpl;

/**
 * Member node in a strong domain. Each member has a weight that reflects the
 * fraction of columns the strong domain occurs in that had a local domain with
 * this term.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class StrongDomainMember extends IdentifiableObjectImpl {
    
    private final BigDecimal _weight;
    
    public StrongDomainMember(int nodeId, BigDecimal weight) {
        
        super(nodeId);
        
        _weight = weight;
    }
    
    public BigDecimal weight() {
        
        return _weight;
    }
}
