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
import java.math.MathContext;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.signature.RobustSignature;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;
import org.opendata.db.eq.EQIndex;

/**
 * Iterative expander for a single column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SingleColumnExpander implements RobustSignatureConsumer {

    private static final Logger LOGGER = Logger
            .getLogger(SingleColumnExpander.class.getName());
        
    private ExpandedColumn _column = null;
    private final int _columnSize;
    private final BigDecimal _decreaseFactor;
    private boolean _done = false;
    private int _expansionSize;
    private int _iteration;
    private final int[] _nodeSizes;
    private final int _numberOfIterations;
    private HashMap<Integer, SupportCounter> _support;
    private final Threshold _threshold;
        
    public SingleColumnExpander(
            EQIndex nodes,
            ExpandedColumn column,
            Threshold threshold,
            BigDecimal decreaseFactor,
            int numberOfIterations
    ) {
        _nodeSizes = nodes.nodeSizes();
        _column = column;
        _numberOfIterations = numberOfIterations;
        _decreaseFactor = decreaseFactor;
        _threshold = threshold;
        
        _done = (_numberOfIterations <= 0);
        _iteration = 0;
        
        int size = 0;
        for (int nodeId : column.originalNodes()) {
            size += _nodeSizes[nodeId];
        }

        if (size == 0) {
            LOGGER.log(Level.WARNING, "Empty column {0} ({1})", new Object[]{column.id(), column.originalNodes().length()});
        }
        
        _columnSize = size;
        _expansionSize = 0;
    }

    public ExpandedColumn column() {

        return _column;
    }

    public boolean isDone() {

        return _done;
    }

    @Override
    public void close() {

        Threshold roundZeroThreshold;
        if (_iteration == 0) {
            roundZeroThreshold = _threshold;
        } else {
            roundZeroThreshold = _threshold.decreaseBy(
                    _decreaseFactor.multiply(new BigDecimal(_iteration))
            );
        }
        
        final int overallSize = _columnSize + _expansionSize;

        HashIDSet expansionNodes = new HashIDSet();
        int expansionSize = _expansionSize;
        
        for (int nodeId : _support.keySet()) {
            SupportCounter sup = _support.get(nodeId);
            BigDecimal orgSup = null;
            try {
                orgSup = sup.originalSupport(_columnSize);
            } catch (java.lang.ArithmeticException ex) {
                LOGGER.log(
                        Level.SEVERE,
                        "Error dividing {0} / {1}",
                        new int[]{sup.originalSupportCount(), _columnSize}
                );
                System.exit(-1);
            }
            if (roundZeroThreshold.isSatisfied(orgSup)) {
                BigDecimal overallSup = null;
                try {
                    overallSup = sup.overallSupport(overallSize);
                }   catch (java.lang.ArithmeticException ex) {
                    LOGGER.log(
                            Level.SEVERE,
                            "Error dividing {0} / {1}",
                            new int[]{sup.overallSupportCount(), overallSize}
                    );
                    System.exit(-1);
                }
                if (_threshold.isSatisfied(overallSup)) {
                    expansionNodes.add(nodeId);
                    expansionSize += _nodeSizes[nodeId];
                }
            }
        }
        
        if (!expansionNodes.isEmpty()) {
            _expansionSize = expansionSize;
            _column = _column.expand(expansionNodes);
        } else {
            _done = true;
        }

        _iteration++;
        if (_iteration >= _numberOfIterations) {
            _done = true;
        }
    }

    @Override
    public void consume(RobustSignature sig) {

        boolean isOriginalNode = _column.isColumnNode(sig.id());
        int weight = _nodeSizes[sig.id()];
        for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
            for (int nodeId : sig.get(iBlock)) {
                if (!_column.contains(nodeId)) {
                    SupportCounter sup;
                    if (_support.containsKey(nodeId)) {
                        sup = _support.get(nodeId);
                    } else {
                        sup = new SupportCounter();
                        _support.put(nodeId, sup);
                    }
                    if (isOriginalNode) {
                        sup.incOriginalSupport(weight);
                    } else {
                        sup.incExpansionSupport(weight);
                    }
                }
            }
        }
    }

    @Override
    public void open() {

        _done = false;        
        _support = new HashMap<>();
    }
    
    public HashMap<Integer, SupportCounter> support() {
        
        return _support;
    }
}
