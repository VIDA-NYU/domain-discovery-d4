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

import java.io.File;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.util.Avg;
import org.opendata.core.util.SimilarityHistogram;

/**
 * Collect statistics for a set of signature blocks. Maintains a histogram of
 * maximum similarity values, counts the number of signatures, the average,
 * maximum and minimum length (in number of nodes and blocks).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureBlocksStats implements SignatureBlocksConsumer {

    private class StatsCollector {
        
        private int _count = 0;
        private int _max = 0;
        private int _min = 0;
        private long _sum;
        
        public void add(int value) {
            
            if (value < _min) {
                _min = value;
            }
            if (value > _max) {
                _max = value;
            }
            _count++;
            _sum += (long)value;
        }
        
        public Avg avg() {
        
            return new Avg(_sum, (long)_count);
        }
        
        public int count() {
            
            return _count;
        }
        
        public int min() {
            
            return _min;
        }
        
        public int max() {
            
            return _max;
        }
        
        public long sum() {
            
            return _sum;
        }
    }
    
    private StatsCollector _blockStats;
    private SimilarityHistogram _histogram = null;
    private StatsCollector[] _nodeStats;
    
    @Override
    public void close() {

    }

    @Override
    public void consume(SignatureBlocks sig) {

        _blockStats.add(sig.size());
        _histogram.add(sig.maxSim());
        
        int[] nodeCount = new int[11];
        for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
            int bl = sig.get(iBlock).length;
            for (int i = 0; i < 10; i++) {
                nodeCount[i] += Math.min(bl, (i + 1) * 10);
            }
            nodeCount[10] += bl;
        }
        
        for (int i = 0; i < nodeCount.length; i++) {
            _nodeStats[i].add(nodeCount[i]);
        }
    }

    @Override
    public boolean isDone() {
        
        return false;
    }

    @Override
    public void open() {

        _blockStats = new StatsCollector();
        _histogram = new SimilarityHistogram();
        _nodeStats = new StatsCollector[11];
        for (int i = 0; i < _nodeStats.length; i++) {
            _nodeStats[i] = new StatsCollector();
        }
    }
    
    public void print(PrintWriter out) {

        out.println("SIGNATURE COUNT: " + _blockStats.count());
        out.println();
        out.println("SIGNATURE BLOCKS");
        out.println("MIN. SIZE      : " + _blockStats.min());
        out.println("MAX. SIZE      : " + _blockStats.max());
        out.println("AVG. SIZE      : " + _blockStats.avg());
        out.println();
        for (int i = 0; i < _nodeStats.length - 1; i++) {
            out.println("NODE COUNTS " + ((i + 1) * 10));
            out.println("MIN. SIZE      : " + _nodeStats[i].min());
            out.println("MAX. SIZE      : " + _nodeStats[i].max());
            out.println("AVG. SIZE      : " + _nodeStats[i].avg());
            out.println("SUM            : " + _nodeStats[i].sum());
        }
        out.println("NODE COUNTS (TOTAL)");
        out.println("MIN. SIZE      : " + _nodeStats[10].min());
        out.println("MAX. SIZE      : " + _nodeStats[10].max());
        out.println("AVG. SIZE      : " + _nodeStats[10].avg());
        out.println("SUM            : " + _nodeStats[10].sum());
        out.flush();
    }
    
    public void print() {

        this.print(new PrintWriter(System.out));
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <signature-blocks-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(SignatureBlocksStats.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File signatureFile = new File(args[0]);
        
        SignatureBlocksStats consumer = new SignatureBlocksStats();

        try (PrintWriter out = new PrintWriter(System.out)) {
            new SignatureBlocksReader(signatureFile).stream(consumer);
            consumer.print(out);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
