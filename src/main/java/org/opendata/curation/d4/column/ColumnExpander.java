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
package org.opendata.curation.d4.column;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;
import org.opendata.curation.d4.SignatureTrimmerFactory;
import org.opendata.core.constraint.Threshold;
import org.opendata.curation.d4.signature.SignatureBlocksDispatcher;
import org.opendata.curation.d4.signature.SignatureBlocksStream;

/**
 * Write set of expanded column equivalence classes to file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnExpander implements Runnable {
    
    private final List<ExpandedColumn> _columns;
    private final ExpandedColumnConsumer _consumer;
    private final BigDecimal _decreaseFactor;
    private final Integer[] _eqTermCounts;
    private final int _id;
    private final int _numberOfIterations;
    private final SignatureBlocksStream _signatures;
    private final Threshold _threshold;
    private final SignatureTrimmerFactory _trimmerFactory;
              
    public ColumnExpander(
            int id,
            Integer[] eqTermCounts,
            List<ExpandedColumn> columns,
            SignatureBlocksStream signatures,
            SignatureTrimmerFactory trimmerFactory,
            Threshold threshold,
            BigDecimal decreaseFactor,
            int numberOfIterations,
            ExpandedColumnConsumer consumer
    ) {
        _id = id;
        _eqTermCounts = eqTermCounts;
        _columns = columns;
        _signatures = signatures;
        _trimmerFactory = trimmerFactory;
        _threshold = threshold;
        _decreaseFactor = decreaseFactor;
        _numberOfIterations = numberOfIterations;
        _consumer = consumer;
    }

    @Override
    public void run() {
        
        System.out.println("TASK " + _id + " EXPAND " + _columns.size() + " COLUMNS");
        
        List<SingleColumnExpander> expanders = new ArrayList<>();
        SignatureBlocksDispatcher dispatcher = new SignatureBlocksDispatcher();
        
        for (ExpandedColumn column : _columns) {
            SingleColumnExpander columnExpander;
            columnExpander = new SingleColumnExpander(
                    _eqTermCounts,
                    column,
                    _threshold,
                    _decreaseFactor,
                    _numberOfIterations
            );
            if (!columnExpander.isDone()) {
                SignatureTrimmer trimmer;
                trimmer = _trimmerFactory
                        .getSignatureTrimmer(
                                column,
                                columnExpander
                        );
                dispatcher.add(trimmer);
                expanders.add(columnExpander);
            } else {
                _consumer.consume(column);
            }
        }
        
        _consumer.open();
        
        int round = 1;
        while (!expanders.isEmpty()) {
            System.out.println(
                    "TASK " + _id + " ROUND " + round + " WITH " + expanders.size() +
                    " ACTIVE EXPANDERS @ " + new Date()
            );
            _signatures.stream(dispatcher);
            ArrayList<SingleColumnExpander> active = new ArrayList<>();
            dispatcher = new SignatureBlocksDispatcher();
            int expansionCount = 0;
            int expandedCount = 0;
            for (SingleColumnExpander expander : expanders) {
                int size = expander.column().expansionSize();
                if (size > 0) {
                    expansionCount += size;
                    expandedCount++;
                }
                if (expander.isDone()) {
                    _consumer.consume(expander.column());
                } else {
                    active.add(expander);
                    SignatureTrimmer trimmer;
                    trimmer = _trimmerFactory
                            .getSignatureTrimmer(
                                    expander.column(),
                                    expander
                            );
                    dispatcher.add(trimmer);
                }
            }
            System.out.println(
                    "TASK " + _id + " EXPANDED " + expandedCount +
                    " OF " + expanders.size() + " COLUMNS WITH " + 
                    expansionCount + " NODES"
            );
            expanders = active;
            round++;
        }

        _consumer.close();
        
        System.out.println("TASK " + _id + " DONE @ " + new Date());
    }
}
