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

import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.curation.d4.signature.trim.ColumnSupportBlockFilter;
import org.opendata.curation.d4.signature.trim.LiberalRobustifier;
import org.opendata.curation.d4.signature.trim.SignatureRobustifier;
import org.opendata.db.eq.CompressedDatabase;
import org.opendata.db.eq.similarity.EQSimilarity;
import org.opendata.db.eq.similarity.JISimilarityArray;

/**
 * Helper class to create instances of D4 components based on configuration
 * parameters.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class D4Config {
   
    private final CompressedDatabase _db;
    
    public D4Config(CompressedDatabase db) {
        
        _db = db;
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
        
        if (identifier.equalsIgnoreCase(EQSimilarity.JI)) {
            return new JISimilarityArray(_db.getEQColumns());
        }
        throw new IllegalArgumentException(
                String.format("Unknown similarity function '%s'", identifier)
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
        
        if (identifier.equalsIgnoreCase(SignatureRobustifier.COLSUPP)) {
            return new ColumnSupportBlockFilter(_db.getEQColumns(), consumer);
        } else if (identifier.equalsIgnoreCase(SignatureRobustifier.LIBERAL)) {
            return new LiberalRobustifier(_db.getEQTermCounts(), consumer);
        }
        throw new IllegalArgumentException(
                String.format("Unknown robustifier '%s'", identifier)
        );
    }
}
