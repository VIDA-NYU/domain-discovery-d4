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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.ImmutableIDSet;
import org.opendata.core.set.ImmutableIdentifiableIDSet;
import org.opendata.core.set.MutableIdentifiableIDSet;
import org.opendata.core.util.Counter;
import org.opendata.core.util.SimpleCounter;

/**
 * Create a set of unique database domains for local domains.
 * 
 * Create the domain set from the results of individual column domain set
 * generator tasks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class UniqueDomainSet implements DomainStream {

    private final ExpandedColumnIndex _columnIndex;
    private final Counter _domainIdFactory;
    private final HashMap<String, IdentifiableIDSet> _domainIndex;
    private final HashObjectSet<MutableIdentifiableIDSet> _domainMapping;
    
    public UniqueDomainSet(ExpandedColumnIndex columnIndex) {
        
        _columnIndex = columnIndex;
        
        _domainIndex = new HashMap<>();
        _domainMapping = new HashObjectSet<>();
        _domainIdFactory = new SimpleCounter();
    }

    public List<Domain> domains() {

        ArrayList<Domain> result = new ArrayList<>();
        
        for (IdentifiableIDSet domain : _domainIndex.values()) {
            IdentifiableIDSet columns = _domainMapping.get(domain.id());
            result.add(new Domain(domain.id(), domain, columns));
        }
        
        return result;
    }
    
    public synchronized void put(int columnId, IDSet nodes) {
        
        IdentifiableIDSet domain;
        
        String key = nodes.toIntString();
        if (_domainIndex.containsKey(key)) {
            domain = _domainIndex.get(key);
        } else {
            domain = new ImmutableIdentifiableIDSet(
                    _domainIdFactory.inc(),
                    new ImmutableIDSet(nodes.toList())
            );
            _domainIndex.put(key, domain);
            _domainMapping.add(new MutableIdentifiableIDSet(domain.id()));
        }
        
        for (int colId : _columnIndex.columns(columnId)) {
            _domainMapping.get(domain.id()).add(colId);
        }
    }

    @Override
    public void stream(DomainConsumer consumer) {

        consumer.open();
        
        for (Domain domain : this.domains()) {
            consumer.consume(domain);
        }
        
        consumer.close();
    }
}
