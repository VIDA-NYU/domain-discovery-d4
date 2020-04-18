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

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.Arguments;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;
import org.opendata.curation.d4.signature.trim.SignatureTrimmerFactory;
import org.opendata.curation.d4.signature.trim.TrimmerType;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermIndexReader;

/**
 * Iterative expander for a single column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SingleColumnExpander implements SignatureBlocksConsumer {

    private static final Logger LOGGER = Logger
            .getLogger(SingleColumnExpander.class.getName());
        
    private class SupportCounter {
        
        private final int _expansionSupport;
        private final int _originalSupport;
        
        public SupportCounter(int[] support) {
            
            _originalSupport = support[0];
            _expansionSupport = support[1];
        }
        
        public int expansionSupportCount() {
            
            return _expansionSupport;
        }
        
        public BigDecimal originalSupport(int size) {
            
            if (size == 0) {
                return BigDecimal.ZERO;
            }

            return new BigDecimal(_originalSupport)
                    .divide(new BigDecimal(size), MathContext.DECIMAL64);
        }
        
        public int originalSupportCount() {
            
            return _originalSupport;
        }
        
        public BigDecimal overallSupport(int size) {
            
            if (size == 0) {
                return BigDecimal.ZERO;
            }
            
            return new BigDecimal(_originalSupport + _expansionSupport)
                    .divide(new BigDecimal(size), MathContext.DECIMAL64);
        }
        
        public int overallSupportCount() {
            
            return _originalSupport + _expansionSupport;
        }
    }

    private ExpandedColumn _column = null;
    private final int _columnSize;
    private final BigDecimal _decreaseFactor;
    private boolean _done = false;
    private HashObjectSet<SupportSet> _expansion = null;
    private final List<IdentifiableObjectSet<SupportSet>> _expansionCollector;
    private int _expansionSize;
    private int _iteration;
    private final int _maxNodeId;
    private final int[] _nodeSizes;
    private final int _numberOfIterations;
    private int[][] _support;
    private final Threshold _threshold;
        
    public SingleColumnExpander(
            ExpandedColumn column,
            Threshold threshold,
            BigDecimal decreaseFactor,
            int numberOfIterations,
            int[] nodeSizes,
            int maxNodeId,
            List<IdentifiableObjectSet<SupportSet>> expansionCollector
    ) {
        _nodeSizes = nodeSizes;
        _column = column;
        _numberOfIterations = numberOfIterations;
        _decreaseFactor = decreaseFactor;
        _threshold = threshold;
        _expansionCollector = expansionCollector;
        
        _maxNodeId = maxNodeId;
        
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
    
    public SingleColumnExpander(
            EQIndex nodes,
            ExpandedColumn column,
            Threshold threshold,
            BigDecimal decreaseFactor,
            int numberOfIterations,
            List<IdentifiableObjectSet<SupportSet>> expansionCollector
    ) {
        this(
                column,
                threshold,
                decreaseFactor,
                numberOfIterations,
                nodes.nodeSizes(),
                nodes.getMaxId(),
                expansionCollector
        );
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
        
        for (int nodeId = 0; nodeId <= _maxNodeId; nodeId++) {
            if (_support[nodeId] != null) {
                SupportCounter sup = new SupportCounter(_support[nodeId]);
                boolean added = false;
                BigDecimal orgSup = null;
                try {
                    orgSup = sup.originalSupport(_columnSize);
                } catch (java.lang.ArithmeticException ex) {
                    LOGGER.log(
                            Level.SEVERE,
                            ex.toString(),
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
                                ex.toString(),
                                new int[]{sup.overallSupportCount(), overallSize}
                        );
                        System.exit(-1);
                    }
                    if (_threshold.isSatisfied(overallSup)) {
                        expansionNodes.add(nodeId);
                        expansionSize += _nodeSizes[nodeId];
                        added = true;
                    }
                }
                if ((_expansion != null) && (added)) {
                    _expansion.get(nodeId).added();
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
    public void consume(SignatureBlocks sig) {

        boolean isOriginalNode = _column.isColumnNode(sig.id());
        int weight = _nodeSizes[sig.id()];
        for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
            for (int nodeId : sig.get(iBlock)) {
                if (!_column.contains(nodeId)) {
                    if (isOriginalNode) {
                        _support[nodeId][0] += weight;
                    } else {
                        _support[nodeId][1] += weight;
                    }
                    if (_expansion != null) {
                        if (!_expansion.contains(nodeId)) {
                            _expansion.add(new SupportSet(nodeId));
                        }
                        _expansion.get(nodeId).add(sig.id());
                    }
                }
            }
        }
    }

    @Override
    public void open() {

        _done = false;        
        _support = new int[_maxNodeId + 1][2];
        if (_expansionCollector != null) {
            _expansion = new HashObjectSet();
            _expansionCollector.add(_expansion);
        }
    }
    
    private static final String ARG_DECREASE = "decrease";
    private static final String ARG_ITERATIONS = "iterations";
    private static final String ARG_THRESHOLD = "threshold";
    private static final String ARG_TRIMMER = "trimmer";
    
    private static final String[] ARGS = {
        ARG_DECREASE,
        ARG_ITERATIONS,
        ARG_THRESHOLD,
        ARG_TRIMMER
    };
    
    private static final String COMMAND =
            "Usage\n" +
            "  --" + ARG_DECREASE + "=<real> [default: 0.05]\n" +
            "  --" + ARG_ITERATIONS + "=<int> [default: 5]\n" +
            "  --" + ARG_THRESHOLD + "=<constraint> [default: GT0.25]\n" +
            "  --" + ARG_TRIMMER + "=<signature-trimmer> [default: " +
                    TrimmerType.CENTRIST.toString() +"]\n" +
            "  <eq-file>\n" +
            "  <signature-file>\n" +
            "  <term-index>\n" +
            "  <column-id>";
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Single Column Expander - Version (" + Constants.VERSION + ")\n");

        if (args.length < 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Arguments params = new Arguments(ARGS, args, 4);
        File eqFile = new File(params.fixedArg(0));
        File signatureFile = new File(params.fixedArg(1));
        File termFile = new File(params.fixedArg(2));
        int columnId = Integer.parseInt(params.fixedArg(3));

        TrimmerType trimmer = TrimmerType
                .valueOf(params.getAsString(ARG_TRIMMER, TrimmerType.CENTRIST.toString()));
        Threshold threshold = Threshold.getConstraint(params.getAsString(ARG_THRESHOLD, "GT0.25"));
        int numberOfIterations = params.getAsInt(ARG_ITERATIONS, 5);
        BigDecimal decreaseFactor = params.getAsBigDecimal(ARG_DECREASE, new BigDecimal("0.05"));
                
        try {
            // Read the node index and the list of columns
            EQIndex nodeIndex = new EQIndex(eqFile);
            Column column = nodeIndex.columns().get(columnId);
            List<IdentifiableObjectSet<SupportSet>> expansionCollector;
            expansionCollector = new ArrayList<>();
            SingleColumnExpander expander = new SingleColumnExpander(
                    nodeIndex,
                    new MutableExpandedColumn(column),
                    threshold,
                    decreaseFactor,
                    numberOfIterations,
                    expansionCollector
            );
            SignatureTrimmerFactory trimmerFactory;
            trimmerFactory = new SignatureTrimmerFactory(nodeIndex, trimmer);
            boolean done = false;
            while (!done) {
                // Add trimmer
                SignatureTrimmer consumer;
                consumer = trimmerFactory
                        .getTrimmer(expander.column().nodes(), expander);
                new SignatureBlocksReader(signatureFile, false).stream(consumer);
                done = expander.isDone();
            }
            // Print expansion information
            HashIDSet nodeFilter = new HashIDSet(column);
            for (IdentifiableObjectSet<SupportSet> expansion : expansionCollector) {
                for (SupportSet node : expansion) {
                    if (node.wasAdded()) {
                        nodeFilter.add(node.id());
                    }
                }
            }
            HashIDSet termFilter = new HashIDSet();
            for (int nodeId : nodeFilter) {
                termFilter.add(nodeIndex.get(nodeId).terms());
            }
            IdentifiableObjectSet<Term> termIndex;
            termIndex = new TermIndexReader(termFile).read(termFilter);
            System.out.println("ORIGINAL COLUMN");
            System.out.println("---------------");
            for (int nodeId : column) {
                for (int termId : nodeIndex.get(nodeId).terms()) {
                    String value = termIndex.get(termId).name();
                    System.out.println(nodeId + "\t" + value);
                }
            }
            System.out.println();
            for (int iRound = 0; iRound < expansionCollector.size(); iRound++) {
                System.out.println("ITERATION " + (iRound + 1));
                System.out.println("-----------");
                boolean hasAdded = false;
                for (SupportSet node : expansionCollector.get(iRound)) {
                    if (node.wasAdded()) {
                        for (int termId : nodeIndex.get(node).terms()) {
                            String value = termIndex.get(termId).name();
                            System.out.println(node.id() + "\t" + value + " (support from " + node.toIntString() + ")");
                        }
                        hasAdded = true;
                    }
                }
                if (!hasAdded) {
                    System.out.println("No further nodes added");
                }
                System.out.println();
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
