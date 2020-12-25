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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.trim.CommonColumnBlockFilter;
import org.opendata.curation.d4.signature.trim.LiberalRobustifier;
import org.opendata.curation.d4.signature.trim.SignatureRobustifier;
import org.opendata.curation.d4.signature.trim.SignatureTrimmerFactory;
import org.opendata.db.column.Column;
import org.opendata.db.eq.CompressedTermIndex;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.similarity.EQSimilarity;
import org.opendata.db.eq.similarity.JISimilarity;
import org.opendata.db.eq.similarity.LogJISimilarity;

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
     * Get the list of all identifier for the equivalence classes in the
     * compressed term index.
     * 
     * @return 
     */
    public Collection<Integer> getEQIdentifiers() {
        
        return _eqIdentifiers;
    }
    
    public SignatureTrimmerFactory getSignatureTrimmerFactory(String identifier) {
       
        return this.getSignatureTrimmerFactory(identifier, this.getColumns());
    }
    
    public SignatureTrimmerFactory getSignatureTrimmerFactory(
            String identifier,
            IdentifiableObjectSet<Column> columns
    ) {
       
        return new SignatureTrimmerFactory(
                _eqTermCounts,
                columns,
                identifier
        );

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
            SignatureBlocksConsumer consumer
    ) {
        
        if (identifier.equalsIgnoreCase(D4Config.ROBUST_COMMONCOL)) {
            return new CommonColumnBlockFilter(this.getColumnsArray(), consumer);
        } else if (identifier.equalsIgnoreCase(D4Config.ROBUST_LIBERAL)) {
            return new LiberalRobustifier(_eqTermCounts, consumer);
        }
        throw new IllegalArgumentException(
                String.format("Unknown robustifier '%s'", identifier)
        );
    }
}
