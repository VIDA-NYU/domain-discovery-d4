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
    }
    
    private StatsCollector _blockStats;
    private SimilarityHistogram _histogram = null;
    private StatsCollector _nodeStats;
    
    @Override
    public void close() {

    }

    @Override
    public void consume(SignatureBlocks sig) {

        _blockStats.add(sig.size());
        _histogram.add(sig.maxSim());
        
        int nodeCount = 0;
        for (int iBlock = 0; iBlock < sig.size(); iBlock++) {
            nodeCount += sig.get(iBlock).length;
        }
        
        _nodeStats.add(nodeCount);
        
    }

    @Override
    public void open() {

        _blockStats = new StatsCollector();
        _histogram = new SimilarityHistogram();
        _nodeStats = new StatsCollector();
    }
    
    public void print(PrintWriter out) {

        out.println("SIGNATURE COUNT: " + _blockStats.count());
        out.println();
        out.println("SIGNATURE BLOCKS");
        out.println("MIN. SIZE      : " + _blockStats.min());
        out.println("MAX. SIZE      : " + _blockStats.max());
        out.println("AVG. SIZE      : " + _blockStats.avg());
        out.println();
        out.println("NODE COUNTS");
        out.println("MIN. SIZE      : " + _nodeStats.min());
        out.println("MAX. SIZE      : " + _nodeStats.max());
        out.println("AVG. SIZE      : " + _nodeStats.avg());
    }
    
    public void print() {

        System.out.println("SIGNATURE COUNT: " + _blockStats.count());
        System.out.println();
        System.out.println("SIGNATURE BLOCKS");
        System.out.println("MIN. SIZE      : " + _blockStats.min());
        System.out.println("MAX. SIZE      : " + _blockStats.max());
        System.out.println("AVG. SIZE      : " + _blockStats.avg());
        System.out.println();
        System.out.println("NODE COUNTS");
        System.out.println("MIN. SIZE      : " + _nodeStats.min());
        System.out.println("MAX. SIZE      : " + _nodeStats.max());
        System.out.println("AVG. SIZE      : " + _nodeStats.avg());
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
