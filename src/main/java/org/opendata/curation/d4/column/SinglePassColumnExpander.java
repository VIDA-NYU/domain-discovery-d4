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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.Arguments;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;
import org.opendata.curation.d4.signature.trim.SignatureTrimmerFactory;
import org.opendata.curation.d4.signature.trim.TrimmerType;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.util.MemUsagePrinter;
import org.opendata.curation.d4.signature.SignatureBlocks;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.SignatureSimilarityFilter;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

/**
 * Expand columns using multiple threads. Makes a single pass over the data
 * and therefore does not support multi-iteration. Reads the data once. Reads
 * chunks of signatures into main memory. Then uses multiple threads to process
 * the signatures.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SinglePassColumnExpander {
    
    public static final String TELEMETRY_ID = "EXPANDED COLUMNS";
    
    private class BufferedWorker implements SignatureBlocksConsumer {

        private final List<SignatureBlocks> _buffer;
        private final int _bufferSize;
        private final List<ColumnExpanderWrapper> _columns;
        private final int _threads;

        // Statistics
        private int _readCount = 0;
        
        public BufferedWorker(
                List<ColumnExpanderWrapper> columns,
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
            ConcurrentLinkedQueue<ColumnExpanderWrapper> queue;
            queue = new ConcurrentLinkedQueue<>(_columns);
            new MemUsagePrinter().print();
            ExecutorService es = Executors.newCachedThreadPool();
            for (int iThread = 0; iThread < _threads; iThread++) {
                ExpanderTask expander = new ExpanderTask(iThread, queue, _buffer);
                es.execute(expander);
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
    
    private class ColumnExpanderWrapper {
    
        private final SingleIterationExpander _expander;
        private final SignatureTrimmer _trimmer;
        
        public ColumnExpanderWrapper(
                SingleIterationExpander expander,
                SignatureTrimmer trimmer
        ) {
            _expander = expander;
            _trimmer = trimmer;
        }
        
        public SingleIterationExpander expander() {
            
            return _expander;
        }
        
        public SignatureTrimmer trimmer() {
            
            return _trimmer;
        }
    }
    
    private class ExpanderTask implements Runnable {

        private final ConcurrentLinkedQueue<ColumnExpanderWrapper> _columns;
        private final int _id;
        private final List<SignatureBlocks> _signatures;

        public ExpanderTask(
                int id,
                ConcurrentLinkedQueue<ColumnExpanderWrapper> columns,
                List<SignatureBlocks> signatures
        ) {
            _id = id;
            _columns = columns;
            _signatures = signatures;
        }
        
        @Override
        public void run() {

            int count = 0;
            
            Date start = new Date();
            
            ColumnExpanderWrapper column;
            while ((column = _columns.poll()) != null) {
                SignatureTrimmer trimmer = column.trimmer();
                for (SignatureBlocks sig : _signatures) {
                    trimmer.consume(sig);
                }
                column.expander().cleanSupport();
                count++;
            }
            
            Date end = new Date();

            long execTime = end.getTime() - start.getTime();
            
            System.out.println(_id + " DONE WITH " + count + " COLUMNS IN " + execTime + " ms");
        }
    }
    
    private final TelemetryCollector _telemetry;
    
    public SinglePassColumnExpander(TelemetryCollector telemetry) {
        
        _telemetry = telemetry;
    }
    
    public SinglePassColumnExpander() {
        
        this(new TelemetryPrinter());
    }
    
    public void run(
            int[] nodeSizes,
            SignatureBlocksReader signatures,
            SignatureTrimmerFactory trimmerFactory,
            IdentifiableObjectSet<Column> db,
            IDSet columnFilter,
            Threshold threshold,
            Threshold sigsim,
            int sigBufferSize,
            int threads,
            ExpandedColumnConsumerFactory consumerFactory
    ) throws java.io.IOException {
        
        HashMap<String, ExpandedColumn> columnIndex = new HashMap<>();
        HashMap<Integer, HashIDSet> groups = new HashMap<>();
        HashMap<String, Integer> mapping = new HashMap<>();
        for (int columnId : columnFilter) {
            Column column = db.get(columnId);
            String key = column.toIntString();
            if (!columnIndex.containsKey(key)) {
                columnIndex.put(key, new MutableExpandedColumn(column));
            } else {
                if (!mapping.containsKey(key)) {
                    int colId = columnIndex.get(key).id();
                    HashIDSet columnSet = new HashIDSet();
                    columnSet.add(colId);
                    columnSet.add(column.id());
                    groups.put(colId, columnSet);
                    mapping.put(key, colId);
                } else {
                    groups.get(mapping.get(key)).add(column.id());
                }
            }
        }
        
        // Sort column in decreasing number of nodes
        List<ExpandedColumn> columns;
        columns = new ArrayList<>(columnIndex.values());
        Collections.sort(columns, (ExpandedColumn c1, ExpandedColumn c2) -> 
                Integer.compare(c1.nodes().length(), c2.nodes().length())
        );
        Collections.reverse(columns);
        
        System.out.println(
                "EXPAND " + columnFilter.length() + " COLUMNS " +
                "IN " + columns.size() + " GROUPS " +
                "USING " + threads + " THREADS"
        );
        
        Date start = new Date();
        System.out.println("START @ " + start);

        List<ColumnExpanderWrapper> expanders = new ArrayList<>();
        for (ExpandedColumn column : columns) {
            SingleIterationExpander expander;
            expander = new SingleIterationExpander(
                    column,
                    threshold,
                    nodeSizes
            );
            SignatureTrimmer trimmer;
            trimmer = trimmerFactory.getTrimmer(column.nodes(), expander);
            expander.open();
            expanders.add(new ColumnExpanderWrapper(expander, trimmer));
        }
        
        System.out.println("START STREAMING @ " + new Date());
        new MemUsagePrinter().print();
        signatures.stream(
                new SignatureSimilarityFilter(
                        sigsim,
                        new BufferedWorker(expanders, sigBufferSize, threads)
                )
        );

        ExpandedColumnConsumer writer = consumerFactory.getConsumer(groups);
        writer.open();

        for (ColumnExpanderWrapper column : expanders) {
            column.trimmer().close();
            writer.consume(column.expander().column());
        }
        System.out.println("DONE READING FOR COLUMN BLOCK @ " + new Date());
        
        writer.close();
        
        Date end = new Date();
        long execTime = end.getTime() - start.getTime();
        _telemetry.add(TELEMETRY_ID, execTime);
        
        System.out.println("END @ " + end);
    }
    
    private static final String ARG_BUFFERSIZE = "buffer";
    private static final String ARG_COLUMNS = "columns";
    private static final String ARG_SIGSIM = "sigsim";
    private static final String ARG_THREADS = "threads";
    private static final String ARG_THRESHOLD = "threshold";
    private static final String ARG_TRIMMER = "trimmer";
    
    private static final String[] ARGS = {
        ARG_BUFFERSIZE,
        ARG_COLUMNS,
        ARG_SIGSIM,
        ARG_THREADS,
        ARG_THRESHOLD,
        ARG_TRIMMER
    };
    
    private static final String COMMAND =
            "Usage\n" +
            "  --" + ARG_BUFFERSIZE + "=<int> [default: 10000]\n" +
            "  --" + ARG_COLUMNS + "=<column-list-file> [default: null]\n" +
            "  --" + ARG_THREADS + "=<int> [default: 6]\n" +
            "  --" + ARG_SIGSIM + "=<constraint> [default: GT0.25]\n" +
            "  --" + ARG_THRESHOLD + "=<constraint> [default: GT0.25]\n" +
            "  --" + ARG_TRIMMER + "=<signature-trimmer> [default: " +
                    TrimmerType.CENTRIST.toString() +"]\n" +
            "  <eq-file>\n" +
            "  <signature-file(s)>\n" +
            "  <output-file-or-directory>";
    
    private static final Logger LOGGER = Logger
            .getLogger(SinglePassColumnExpander.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Single Pass Column Expander - Version (" + Constants.VERSION + ")\n");

        if (args.length < 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Arguments params = new Arguments(ARGS, args, 3);
        File eqFile = new File(params.fixedArg(0));
        File signatureFile = new File(params.fixedArg(1));
        File output = new File(params.fixedArg(2));

        TrimmerType trimmer = TrimmerType
                .valueOf(params.getAsString(ARG_TRIMMER, TrimmerType.CENTRIST.toString()));
        Threshold sigsim = Threshold.getConstraint(params.getAsString(ARG_SIGSIM, "GT0.25"));
        Threshold threshold = Threshold.getConstraint(params.getAsString(ARG_THRESHOLD, "GT0.25"));
        int sigBufferSize = params.getAsInt(ARG_BUFFERSIZE, 10000);
        int threads = params.getAsInt(ARG_THREADS, 6);
                
        File columnsFile = null;
        if (params.has(ARG_COLUMNS)) {
            columnsFile = new File(params.get(ARG_COLUMNS));
        }
        
        try {
            // Read the node index and the list of columns
            EQIndex nodeIndex = new EQIndex(eqFile);
            IdentifiableObjectSet<Column> db = nodeIndex.columns();
            int[] nodeSizes = nodeIndex.nodeSizes();
            // Read the list of column identifier if a columns file was given
            IDSet columnFilter;
            if (columnsFile != null) {
                columnFilter = new HashIDSet(columnsFile);
            } else {
                columnFilter = db.keys();
            }
            new SinglePassColumnExpander().run(
                    nodeSizes,
                    new SignatureBlocksReader(signatureFile),
                    new SignatureTrimmerFactory(nodeSizes, trimmer),
                    db,
                    columnFilter,
                    threshold,
                    sigsim,
                    sigBufferSize,
                    threads,
                    new ExpandedColumnWriterFactory(output, false)
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
