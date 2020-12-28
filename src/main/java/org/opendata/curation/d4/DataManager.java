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
package org.opendata.curation.d4;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.object.IdentifiableInteger;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.signature.ContextSignatureBlocksConsumer;
import org.opendata.curation.d4.signature.trim.CommonColumnBlockFilter;
import org.opendata.curation.d4.signature.trim.IgnoreLastBlockRobustifier;
import org.opendata.curation.d4.signature.trim.LiberalRobustifier;
import org.opendata.curation.d4.signature.trim.SignatureRobustifier;
import org.opendata.db.column.Column;
import org.opendata.db.eq.CompressedTermIndex;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.similarity.EQSimilarity;
import org.opendata.db.eq.similarity.JISimilarity;
import org.opendata.db.eq.similarity.LogJISimilarity;
import org.opendata.db.eq.similarity.WeightedJISimilarity;

/**
 * The D4 data manager is a wrapper around the set of equivalence classes in
 * the dataset that the domain discovery algorithm operates on. This class
 * provides the functionality to create instances of D4 components based on
 * configuration parameters.
 * 
 * If converted into an interface, the data manager provides the option to
 * support (or test) different implementations for data representation.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DataManager {
    
    private HashObjectSet<Column> _columns = null;
    private Integer[][] _eqColumns = null;
    private IdentifiableDouble[][] _eqColumnWeights = null;
    private final List<Integer> _eqIdentifiers;
    private final CompressedTermIndex _eqIndex;
    private final Integer[] _eqTermCounts;
    private final int _maxEqIdentifier;
    
    public DataManager(CompressedTermIndex eqIndex) {
        
        _eqIndex = eqIndex;
        
        HashMap<Integer, Integer> eqSizes = new HashMap<>();
        int maxId = -1;
        for (EQ eq : eqIndex) {
            int id = eq.id();
            eqSizes.put(id, eq.termCount());
            if (id > maxId) {
                maxId = id;
            }
        }
        
        _maxEqIdentifier = maxId;
        _eqIdentifiers = new ArrayList<>(eqSizes.keySet());
        _eqTermCounts = new Integer[maxId + 1];
        for (Integer key : eqSizes.keySet()) {
            _eqTermCounts[key] = eqSizes.get(key);
        }
    }
    
    /**
     * Get the set of columns in the database. Each column contains a set of
     * equivalence class identifier.
     * @return 
     */
    public IdentifiableObjectSet<Column> getColumns() {
        
        if (_columns == null) {
            _columns = new HashObjectSet<>();
            for (EQ eq : _eqIndex) {
                for (int columnId : eq.columns()) {
                    Column column;
                    if (!_columns.contains(columnId)) {
                        column = new Column(columnId);
                        _columns.add(column);
                    } else {
                        column = _columns.get(columnId);
                    }
                    column.add(eq.id());
                }
            }
        }
        return _columns;
    }

    /**
     * Get the list of all identifier for the equivalence classes in the
     * compressed term index.
     * 
     * @return 
     */
    public Collection<Integer> getEQIdentifiers() {
        
        return _eqIdentifiers;
    }
    
    /**
     * Get the similarity function for equivalence classes that is referenced
     * by the given identifier. The following identifier are currently
     * recognized:
     * 
     * - JI
     * 
     * If an unknown identifier is given an exception is raised.
     * 
     * @param identifier
     * @return 
     */
    public EQSimilarity getEQSimilarityFunction(String identifier) {
                
        if (identifier.equalsIgnoreCase(D4Config.EQSIM_JI)) {
            return new JISimilarity(this.getColumnsArray());
        } else if (identifier.equalsIgnoreCase(D4Config.EQSIM_LOGJI)) {
            return new LogJISimilarity(this.getColumnsArray());
        } else if (identifier.equalsIgnoreCase(D4Config.EQSIM_TFICF)) {
            return new WeightedJISimilarity(this.getColumnWeightsArray());
        }
        throw new IllegalArgumentException(
                String.format("Unknown similarity function '%s'", identifier)
        );
    }
    
    /**
     * Get a mapping of equivalence class identifier to their term counts.
     * 
     * @return 
     */
    public Integer[] getEQTermCounts() {
    
        return _eqTermCounts;
    }
    
    /**
     * Get multi-dimensional array that contains the list of all columns for
     * each equivalence class.
     * 
     * @return 
     */
    private Integer[][] getColumnsArray() {
        
        if (_eqColumns == null) {
            _eqColumns = new Integer[_maxEqIdentifier + 1][];
            for (EQ eq : _eqIndex) {
                _eqColumns[eq.id()] = eq.columns();
            }
        }
        
        return _eqColumns;
    }
    
    /**
     * Get multi-dimensional array that contains the list of all columns for
     * each equivalence class together with their weight. The weight for each
     * equivalence class is an adoption of the tf-idf measure. We consider each
     * column as a document (hence the name TF-ICF for Inverted Column Frequency).
     * The measures for tf and icf are based on :
     * 
     * https://en.wikipedia.org/wiki/Tf%E2%80%93idf
     * 
     * We use the augmented term frequency to prevent bias towards larger
     * columns.
     * 
     * @return 
     */
    private IdentifiableDouble[][] getColumnWeightsArray() {
        
        if (_eqColumnWeights == null) {
            // Get the total number of columns and the most requent equivalence
            // class for each column.
            HashMap<Integer, Integer> columns = new HashMap<>();
            int maxColId = -1;
            for (EQ eq : _eqIndex) {
                for (IdentifiableInteger col : eq.columnFrequencies()) {
                    if (col.id() > maxColId) {
                        maxColId = col.id();
                    }
                    Integer maxFreq = columns.get(col.id());
                    if (maxFreq != null) {
                        if (col.value() > maxFreq) {
                            columns.put(col.id(), col.value());
                        }
                    } else {
                        columns.put(col.id(), col.value());
                    }
                }
            }
            // Create an array for the max. frequent count of each column.
            Integer[] maxFreq = new Integer[maxColId + 1];
            for (Integer colId : columns.keySet()) {
                maxFreq[colId] = columns.get(colId);
            }
            int columnCount = columns.size();
            // Compute weights for each equivalence class.
            _eqColumnWeights = new IdentifiableDouble[_maxEqIdentifier + 1][];
            for (EQ eq : _eqIndex) {
                IdentifiableInteger[] eqColFreqs = eq.columnFrequencies();
                IdentifiableDouble[] weights = new IdentifiableDouble[eqColFreqs.length];
                for (int iCol = 0; iCol < eqColFreqs.length; iCol++) {
                    IdentifiableInteger col = eqColFreqs[iCol];
                    double tf = 0.5 + (0.5 * ((double)col.value() / (double)maxFreq[col.id()]));
                    double icf = Math.log((double)columnCount / (double)eqColFreqs.length);
                    weights[iCol] = new IdentifiableDouble(col.id(), tf * icf);
                }
                _eqColumnWeights[eq.id()] = weights;
            }
        }
        
        return _eqColumnWeights;
    }
    
    /**
     * Get signature robustifier that is referenced by the given identifier. The
     * following identifier are currently recognized:
     * 
     * - COLUMN-SUPPORT
     * - LIBERAL
     * 
     * @param identifier
     * @param consumer
     * @return 
     */
    public SignatureRobustifier getSignatureRobustifier(
            String identifier,
            ContextSignatureBlocksConsumer consumer
    ) {
        
        if (identifier.equalsIgnoreCase(D4Config.ROBUST_COMMONCOL)) {
            return new CommonColumnBlockFilter(this.getColumnsArray(), consumer);
        } else if (identifier.equalsIgnoreCase(D4Config.ROBUST_LIBERAL)) {
            return new LiberalRobustifier(consumer);
        } else if (identifier.equalsIgnoreCase(D4Config.ROBUST_IGNORELAST)) {
            return new IgnoreLastBlockRobustifier(consumer);
        }
        throw new IllegalArgumentException(
                String.format("Unknown robustifier '%s'", identifier)
        );
    }
    
    public SignatureTrimmerFactory getSignatureTrimmerFactory(
            String identifier,
            boolean originalOnly
    ) {
        
        return new SignatureTrimmerFactory(identifier, _eqTermCounts, originalOnly);
    }
}
