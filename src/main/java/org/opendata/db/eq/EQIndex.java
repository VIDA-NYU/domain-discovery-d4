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
package org.opendata.db.eq;

import java.io.File;
import java.io.PrintWriter;
import org.opendata.core.graph.Node;
import org.opendata.core.io.FileSystem;
import org.opendata.core.prune.SizeFunction;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.util.IdentifiableCount;
import org.opendata.core.util.IdentifiableCounterSet;
import org.opendata.db.Database;
import org.opendata.db.column.Column;

/**
 * Index of equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQIndex extends HashObjectSet<EQ> implements EQStream, SizeFunction {

    private final File _file;
    private int[] _nodeSizes = null;
    
    public EQIndex(File eqFile, EQFactory factory) throws java.io.IOException {
        
        super(new EQReader(eqFile, factory).read());
        
        _file = eqFile;
    }
    
    public EQIndex(File eqFile) throws java.io.IOException {
        
        super(new EQReader(eqFile).read());
        
        _file = eqFile;
    }
    
    public int[] columnSizes() {
        
        IdentifiableCounterSet columns = new IdentifiableCounterSet();
        for (EQ node : this) {
            for (int columnId : node.columns()) {
                columns.inc(columnId, node.terms().length());
            }
        }
        
        int[] values = new int[columns.getMaxId() + 1];
        for (IdentifiableCount column : columns) {
            values[column.id()] = column.value();
        }
        return values;
    }
    
    public IdentifiableObjectSet<IdentifiableIDSet> columns() {
        
        HashObjectSet<IdentifiableIDSet> columns = new HashObjectSet<>();
        for (Column column : new Database(this).columns()) {
            columns.add(column);
        }
        return columns;
    }
    
    @Override
    public int getSize(int id) {

        return this.nodeSizes()[id];
    }
    
    public IdentifiableObjectSet<Node> nodes() {
        
        HashObjectSet nodes = new HashObjectSet<>();
        for (EQ eq : this) {
            nodes.add(new Node(eq.id(), eq.columns().toArray()));
        }
        return nodes;
    }
    
    public synchronized int[] nodeSizes() {
        
        if (_nodeSizes == null) {
            _nodeSizes = new int[this.getMaxId() + 1];
            for (EQ node : this) {
                _nodeSizes[node.id()] = node.terms().length();
            }
        }
        return _nodeSizes;
    }
    
    /**
     * Distribute the identifier of all columns in the database across a given
     * number of files. The files will be created in the given output directory.
     * All files are named by the prefix, followed by a '.' and the file number.
     * The suffix for all files in '.txt'.
     * 
     * @param numberOfFiles
     * @param namePrefix
     * @param outputDir
     * @throws java.io.IOException 
     */
    public void splitColumns(
            int numberOfFiles,
            String namePrefix,
            File outputDir
    ) throws java.io.IOException {
        
        // Create the output folder if it does not exist
        FileSystem.createFolder(outputDir);
        
        PrintWriter[] writers = new PrintWriter[numberOfFiles];
        for (int iFile = 0; iFile < numberOfFiles; iFile++) {
            String filename = namePrefix + "." + iFile + ".txt";
            File file = FileSystem.joinPath(outputDir, filename);
            writers[iFile] = FileSystem.openPrintWriter(file);
        }
        
        int index = 0;
        for (int columnId : this.columns().keys()) {
            writers[index].println(columnId);
            index = (index + 1) % writers.length;
        }
        
        for (PrintWriter out : writers) {
            out.close();
        }
    }
    
    /**
     * Distribute the identifier of all nodes in the index across a given number
     * of files. The files will be created in the given output directory. All
     * files are named by the prefix, followed by a '.' and the file number.
     * The suffix for all files in '.txt'.
     * 
     * @param numberOfFiles
     * @param namePrefix
     * @param outputDir
     * @throws java.io.IOException 
     */
    public void splitNodes(
            int numberOfFiles,
            String namePrefix,
            File outputDir
    ) throws java.io.IOException {
        
        // Create the output folder if it does not exist
        FileSystem.createFolder(outputDir);
        
        PrintWriter[] writers = new PrintWriter[numberOfFiles];
        for (int iFile = 0; iFile < numberOfFiles; iFile++) {
            String filename = namePrefix + "." + iFile + ".txt";
            File file = FileSystem.joinPath(outputDir, filename);
            writers[iFile] = FileSystem.openPrintWriter(file);
        }
        
        int index = 0;
        for (int nodeId : this.keys()) {
            writers[index].println(nodeId);
            index = (index + 1) % writers.length;
        }
        
        for (PrintWriter out : writers) {
            out.close();
        }
    }

    @Override
    public void stream(EQConsumer consumer) {

        consumer.open();
        
        for (EQ node : this) {
            consumer.consume(node);
        }
        consumer.close();
    }
}
