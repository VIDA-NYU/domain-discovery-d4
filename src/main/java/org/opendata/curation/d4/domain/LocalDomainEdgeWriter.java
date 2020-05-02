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
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.constraint.GreaterThanConstraint;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.constraint.ZeroThreshold;
import org.opendata.curation.d4.Arguments;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.curation.d4.signature.SignatureBlocksStream;
import org.opendata.curation.d4.signature.trim.TrimmerType;
import org.opendata.core.io.FileSystem;
import org.opendata.core.io.SynchronizedWriter;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.util.MemUsagePrinter;
import org.opendata.curation.d4.domain.graph.HexEdgeWriter;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureSimilarityFilter;
import org.opendata.curation.d4.signature.trim.CentristTrimmer;
import org.opendata.curation.d4.signature.trim.ConservativeTrimmer;
import org.opendata.curation.d4.signature.trim.LiberalTrimmer;
import org.opendata.curation.d4.signature.trim.NonTrimmer;
import org.opendata.curation.d4.signature.trim.PrecisionScore;
import org.opendata.db.eq.EQIndex;

/**
 * Write edges for all nodes in an expanded column. Trims the signature of each
 * column member and writes the identifier of the referenced column nodes to
 * file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LocalDomainEdgeWriter {
                   
    private class BufferedWorker implements SignatureBlocksConsumer {

        private final List<SignatureBlocks> _buffer;
        private final int _bufferSize;
        private final List<SignatureBlocksConsumer> _columns;
        private final int _threads;

        // Statistics
        private int _readCount = 0;
        
        public BufferedWorker(
                List<SignatureBlocksConsumer> columns,
                int bufferSize,
                int threads
        ) {
            _columns = columns;
            _bufferSize = bufferSize;
            _threads = threads;
            
            _buffer = new ArrayList<>(bufferSize);
        }
        
        @Override
        public void close() {

            if (!_buffer.isEmpty()) {
                this.processBuffer();
            }
        }

        @Override
        public void consume(SignatureBlocks sig) {

            _buffer.add(sig);
            _readCount++;
            if (_buffer.size() == _bufferSize) {
                this.processBuffer();
            }
        }

        @Override
        public void open() {

        }
        
        private void processBuffer() {
        
            System.out.println("PROCESS BUFFER OF " + _buffer.size() + " SIGNATURES (" + _readCount + ") @ " + new Date());
            ConcurrentLinkedQueue<SignatureBlocks> queue;
            queue = new ConcurrentLinkedQueue<>(_buffer);
            new MemUsagePrinter().print();
            ExecutorService es = Executors.newCachedThreadPool();
            for (int iThread = 0; iThread < _threads; iThread++) {
                es.execute(new EdgeWriterTask(_columns, queue));
            }
            es.shutdown();
            try {
                es.awaitTermination(_threads, TimeUnit.DAYS);
            } catch (java.lang.InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            
            System.out.println("DONE BUFFER PROCESSING @ " + new Date());
        
            _buffer.clear();
        }
    }

    
    private class EdgeWriterTask implements Runnable {

        private final List<SignatureBlocksConsumer> _columns;
        private final ConcurrentLinkedQueue<SignatureBlocks> _signatures;
        
        public EdgeWriterTask(
                List<SignatureBlocksConsumer> columns,
                ConcurrentLinkedQueue<SignatureBlocks> signatures
       ) {
            _columns = columns;
            _signatures = signatures;
        }
        
        @Override
        public void run() {

            SignatureBlocks sig;
            while ((sig = _signatures.poll()) != null) {
                for (SignatureBlocksConsumer column : _columns) {
                    column.consume(sig);
                }
            }
        }
    }
    
    public void run(
            EQIndex eqIndex,
            ExpandedColumnIndex columnIndex,
            SignatureBlocksStream signatures,
            String trimmer,
            Threshold sigsim,
            int threads,
            SynchronizedWriter out
    ) throws java.io.IOException {

        Date start = new Date();
        System.out.println("START @ " + start);

        TrimmerType trimmerType;
        Threshold threshold;
        
        if (trimmer.contains(":")) {
            String[] tokens = trimmer.split(":");
            trimmerType = TrimmerType.valueOf(tokens[0]);
            threshold = Threshold.getConstraint(tokens[1]);
        } else {
            trimmerType = TrimmerType.valueOf(trimmer);
            threshold = new GreaterThanConstraint(BigDecimal.ZERO);
        }
        
        int[] nodeSizes = eqIndex.nodeSizes();

        List<SignatureBlocksConsumer> workers = new ArrayList<>();
        for (ExpandedColumn column : columnIndex.columns()) {
            SignatureBlocksConsumer consumer;
            consumer = new HexEdgeWriter(column, out);
            switch (trimmerType) {
                case CONSERVATIVE:
                    consumer = new ConservativeTrimmer(column.nodes(), consumer);
                    break;
                case CENTRIST:
                    consumer = new CentristTrimmer(
                            column.nodes(),
                            nodeSizes,
                            new PrecisionScore(),
                            new MaxDropFinder<>(threshold, false, false),
                            new ZeroThreshold(),
                            consumer
                    );
                    consumer = new LiberalTrimmer(eqIndex.nodeSizes(), consumer);
                    break;
                default:
                    consumer = new NonTrimmer(column.nodes(), consumer);
            }
            consumer.open();
            workers.add(consumer);
        }

        SignatureBlocksConsumer sigConsumer;
        sigConsumer = new BufferedWorker(workers, 50000, threads);
        if (!sigsim.isSatisfied(BigDecimal.ZERO)) {
            sigConsumer = new SignatureSimilarityFilter(sigsim, sigConsumer);
        }
        
        signatures.stream(sigConsumer);

        Date end = new Date();
        System.out.println("END @ " + end);
    }
    
    private static final String ARG_COLUMNS = "columns";
    private static final String ARG_SIGSIM = "sigsim";
    private static final String ARG_THREADS = "threads";
    private static final String ARG_TRIMMER = "trimmer";
    
    private static final String[] ARGS = {
        ARG_COLUMNS,
        ARG_SIGSIM,
        ARG_THREADS,
        ARG_TRIMMER
    };
    
    private static final String COMMAND =
            "Usage\n" +
            "  --" + ARG_COLUMNS + "=<column-list-file> [default: null]\n" +
            "  --" + ARG_SIGSIM + "=<constraint> [default: GT0.0]\n" +
            "  --" + ARG_THREADS + "=<int> [default: 6]\n" +
            "  --" + ARG_TRIMMER + "=<signature-trimmer> [default: " +  
                    TrimmerType.CENTRIST.toString() +":GT0.1]\n" +
            "  <eq-file>\n" +
            "  <signature-file(s)>\n" +
            "  <columns-file>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(LocalDomainEdgeWriter.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Local Domain Edge Writer - Version (" + Constants.VERSION + ")\n");

        if (args.length < 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Arguments params = new Arguments(ARGS, args, 4);
        File eqFile = new File(params.fixedArg(0));
        File signatureFile = new File(params.fixedArg(1));
        File columnFile = new File(params.fixedArg(2));
        File outputFile = new File(params.fixedArg(3));

        String trimmer = params
                .getAsString(ARG_TRIMMER, TrimmerType.CENTRIST.toString() + ":GT0.1");
        Threshold sigsim = Threshold.getConstraint(params.getAsString(ARG_SIGSIM, "GT0.0"));
        int threads = params.getAsInt(ARG_THREADS, 6);
                
        File columnsFile = null;
        if (params.has(ARG_COLUMNS)) {
            columnsFile = new File(params.get(ARG_COLUMNS));
        }
        
        FileSystem.createParentFolder(outputFile);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            // Read the node index and the list of columns
            EQIndex nodeIndex = new EQIndex(eqFile);
            // Read the list of column identifier if a columns file was given
            ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
            if (columnsFile != null) {
                 new ExpandedColumnReader(columnFile)
                         .stream(columnIndex, new HashIDSet(columnsFile));
            } else {
                 new ExpandedColumnReader(columnFile).stream(columnIndex);
            }
            new LocalDomainEdgeWriter().run(
                    nodeIndex,
                    columnIndex,
                    new SignatureBlocksReader(signatureFile),
                    trimmer,
                    sigsim,
                    threads,
                    new SynchronizedWriter(out)
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
