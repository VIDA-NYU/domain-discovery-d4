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

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.core.util.count.IdentifiableCounterSet;

/**
 * Print statistics for a set of domains.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainSetStatsPrinter implements DomainConsumer {

    private IdentifiableCounterSet _columnDomainCount = null;
    private HashIDSet _columnsWithDomains = null;
    private int _domainColumnCount = 0;
    private int _domainCount = 0;
    private int _domainNodeCount = 0;
    private int _maxDomain = 0;

    @Override
    public void close() {

    }

    @Override
    public void consume(Domain domain) {

        _domainCount++;
        int domLen = domain.length();
        _domainNodeCount += domLen;
        if (domLen > _maxDomain) {
            _maxDomain = domLen;
        }
        IDSet cols = domain.columns();
        _columnsWithDomains.add(cols);
        _domainColumnCount += cols.length();
        for (int columnId : cols) {
            _columnDomainCount.inc(columnId);
        }
    }

    @Override
    public void open() {

        _columnDomainCount = new IdentifiableCounterSet();
        _columnsWithDomains = new HashIDSet();
        _domainColumnCount = 0;
        _domainCount = 0;
        _domainNodeCount = 0;
        _maxDomain = 0;
    }

    public void print() {
        
	System.out.println("NUMBER OF DOMAINS      : " + _domainCount);
	System.out.println("COLUMNS WITH DOMAINS   : " + _columnsWithDomains.length());
        if (_domainCount > 0) {
            System.out.println("MAX. DOMAIN SIZE       : " + _maxDomain);
            System.out.println("AVG. DOMAIN SIZE       : " + new FormatedBigDecimal((double)_domainNodeCount/(double)_domainCount));
            System.out.println();
            System.out.println("MAX. DOMAINS PER COLUMN: " + _columnDomainCount.getMaxValue());
            System.out.println("AVG. DOMAINS PER COLUMN: " + new FormatedBigDecimal((double)_domainColumnCount/(double)_columnDomainCount.size()));
            System.out.println("AVG. COLUMNS PER DOMAIN: " + new FormatedBigDecimal((double)_domainColumnCount/(double)_domainCount));
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <domain-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(DomainSetStatsPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File domainFile = new File(args[0]);
        
        DomainSetStatsPrinter stats = new DomainSetStatsPrinter();
        
        try {
            new DomainReader(domainFile).stream(stats);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
        
        stats.print();
    }
}
