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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Date;
import java.util.HashMap;
import org.opendata.curation.d4.SignatureTrimmerFactory;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.signature.SignatureBlocksStream;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;
import org.opendata.db.Database;
import org.opendata.db.EQTerms;

/**
 * Generator for local domains in an expanded column.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LocalDomainGenerator {
    
    private final Database _db;
    private final Integer[] _eqTermCounts;
    private final SignatureBlocksStream _signatures;

    public LocalDomainGenerator(
            Database db,
            SignatureBlocksStream signatures,
            Integer[] eqTermCounts
    ) {
        _db = db;
        _signatures = signatures;
        _eqTermCounts = eqTermCounts;
    }
    
    public JsonObject getLocalDomain(
            ExpandedColumn column,
            SignatureTrimmerFactory trimmerFactory
    ) {
        
        UniqueDomainSet domains;
        domains = new UniqueDomainSet(new ExpandedColumnIndex(column));
        
        UndirectedDomainGenerator domainGenerator;
        domainGenerator = new UndirectedDomainGenerator(
                column,
                domains,
                _eqTermCounts
        );
        SignatureTrimmer trimmer;
        trimmer = trimmerFactory.getSignatureTrimmer(column, domainGenerator);
        Date runStart = new Date();
        _signatures.stream(trimmer);
        Date runEnd = new Date();
        
        HashMap<Integer, EQTerms> termIndex = _db.read(column.nodes(), 10);
        
        JsonObject doc = new JsonObject();
        // Column.
        JsonObject col = new JsonObject();
        col.add("original", column.originalNodes().toJsonArray());
        col.add("expansion", column.expandedNodes().toJsonArray());
        doc.add("column", col);
        // Domains
        JsonArray arrDomains = new JsonArray();
        for (Domain domain : domains.domains()) {
            JsonArray arrDomain = new JsonArray();
            for (int eqId : domain) {
                arrDomain.add(termIndex.get(eqId).toJsonObject());
            }
            arrDomains.add(arrDomain);
        }
        doc.add("domains", arrDomains);
        // Exec. Time
        doc.add("execTime", new JsonPrimitive(runEnd.getTime() - runStart.getTime()));
        return doc;
    }
}
