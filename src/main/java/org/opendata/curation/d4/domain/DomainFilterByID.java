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

import org.opendata.core.object.ObjectFilter;

/**
 * Filter domains based on their domain identifier. Uses an object filter to
 * only pass those domain on to a downstream consumer that have identifier that
 * satisfy the filter condition (i.e., occur in the filter set).
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainFilterByID implements DomainConsumer {

    private final DomainConsumer _consumer;
    private final ObjectFilter<Integer> _filter;
    
    public DomainFilterByID(ObjectFilter<Integer> filter, DomainConsumer consumer) {
        
        _filter = filter;
        _consumer = consumer;
    }
    
    @Override
    public void close() {

        _consumer.close();
    }

    @Override
    public void consume(Domain domain) {

        if (_filter.contains(domain.id())) {
            _consumer.consume(domain);
        }
    }

    @Override
    public void open() {

        _consumer.open();
    }
}
