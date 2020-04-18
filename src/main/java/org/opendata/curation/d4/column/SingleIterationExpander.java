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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.similarity.Support;
import org.opendata.core.util.count.Counter;

/**
 * Simplified version of the single column expander. This expander does only a
 * single round of expansion.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SingleIterationExpander implements SignatureBlocksConsumer {

    private static final Logger LOGGER = Logger
            .getLogger(SingleIterationExpander.class.getName());
        
    private ExpandedColumn _column = null;
    private final int _columnSize;
    private final boolean _lowMemUsage;
    private final int[] _nodeSizes;
    private int _remainingWeight;
    private HashMap<Integer, Counter> _support;
    private final Threshold _threshold;
    
    // Telemetry
    private int _sigCount = 0;
    private long _cleanTime = 0;
    private long _supportTime = 0;
    
    public SingleIterationExpander(
            ExpandedColumn column,
            Threshold threshold,
            int[] nodeSizes,
            boolean lowMemUsage
    ) {
        _nodeSizes = nodeSizes;
        _column = column;
        _threshold = threshold;
        _lowMemUsage = lowMemUsage;
        
        int size = 0;
        for (int nodeId : column.originalNodes()) {
            size += _nodeSizes[nodeId];
        }

        if (size == 0) {
            LOGGER.log(
                    Level.WARNING,
                    "Empty column {0} ({1})",
                    new Object[]{column.id(), column.originalNodes().length()}
            );
        }
        
        _columnSize = size;
        _remainingWeight = _columnSize;
    }

    public ExpandedColumn column() {

        return _column;
    }

    @Override
    public void close() {

        HashIDSet expansionNodes = new HashIDSet();
        
        for (int nodeId : _support.keySet()) {
            Support sup = new Support(_support.get(nodeId).value(), _columnSize);
            if (_threshold.isSatisfied(sup.value())) {
                expansionNodes.add(nodeId);
            }
        }
        
        if (!expansionNodes.isEmpty()) {
            _column = _column.expand(expansionNodes);
        }
        
        System.out.println(_column.id() + "\t" + _sigCount + "\t" + _supportTime + "\t" + _cleanTime);
    }

    @Override
    public void consume(SignatureBlocks sig) {

        if (_column.isColumnNode(sig.id())) {
            int weight = _nodeSizes[sig.id()];
            _remainingWeight -= weight;
            HashSet<Integer> positiveNodes = new HashSet<>();
            long t0 = new Date().getTime();
            for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
                for (int nodeId : sig.get(iBlock)) {
                    if (!_column.contains(nodeId)) {
                        if (!_support.containsKey(nodeId)) {
                            // Only add nodes that can satisfy the support
                            // constraint.
                            int ub = _remainingWeight + weight;
                            Support sup = new Support(ub, _columnSize);
                            if (_threshold.isSatisfied(sup.value())) {
                                _support.put(nodeId, new Counter(weight));
                                positiveNodes.add(nodeId);
                            }
                        } else {
                            Counter counter = _support.get(nodeId);
                            int ub = counter.value() + _remainingWeight + weight;
                            Support sup = new Support(ub, _columnSize);
                            if (_threshold.isSatisfied(sup.value())) {
                                counter.inc(weight);
                                positiveNodes.add(nodeId);
                            } else {
                                _support.remove(nodeId);
                            }
                        }
                    }
                }
            }
            long t1 = new Date().getTime();
            // Remove nodes that cannot meet the threshold constraint anymore.
            if (_lowMemUsage) {
                List<Integer> nodes = new ArrayList<>(_support.keySet());
                for (int nodeId : nodes) {
                    if (!positiveNodes.contains(nodeId)) {
                        Counter counter = _support.get(nodeId);
                        int ub = counter.value() + _remainingWeight;
                        Support sup = new Support(ub, _columnSize);
                        if (!_threshold.isSatisfied(sup.value())) {
                            _support.remove(nodeId);
                        }
                    }
                }
                long t2 = new Date().getTime();
                _cleanTime += (t2 = t1);
            }
            _sigCount++;
            _supportTime += (t1 - t0);
       }
    }

    @Override
    public void open() {

        _support = new HashMap<>();
    }
}
