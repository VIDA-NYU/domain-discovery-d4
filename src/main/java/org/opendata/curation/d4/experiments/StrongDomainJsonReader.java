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
package org.opendata.curation.d4.experiments;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.MutableIdentifiableIDSet;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class StrongDomainJsonReader {
 
    public MutableIdentifiableIDSet read(
            File file,
            boolean firstBlockOnly
    ) throws java.io.IOException {
        
        JsonArray blocks;
        blocks = new JsonParser()
                .parse(new FileReader(file))
                .getAsJsonObject()
                .get("terms")
                .getAsJsonArray();
        
        int domainId = Integer.parseInt(file.getName().substring(0, file.getName().indexOf(".")));
        HashIDSet terms = new HashIDSet();
        
        int blockCount = 1;
        if (!firstBlockOnly) {
            blockCount = blocks.size();
        }
        for (int iBlock = 0; iBlock < blockCount; iBlock++) {
            JsonArray block = blocks.get(iBlock).getAsJsonArray();
            for (int iTerm = 0; iTerm < block.size(); iTerm++) {
                int termId = block.get(iTerm).getAsJsonObject().get("id").getAsInt();
                terms.add(termId);
            }
        }
        return new MutableIdentifiableIDSet(domainId, terms);
    }
 
    public List<IDSet> readBlocks(File file) throws java.io.IOException {
        
        JsonArray blocks;
        blocks = new JsonParser()
                .parse(new FileReader(file))
                .getAsJsonObject()
                .get("terms")
                .getAsJsonArray();
        
        int domainId = Integer.parseInt(file.getName().substring(0, file.getName().indexOf(".")));
        List<IDSet> terms = new ArrayList<>();
        
        for (int iBlock = 0; iBlock < blocks.size(); iBlock++) {
            JsonArray block = blocks.get(iBlock).getAsJsonArray();
            HashIDSet blockTerms = new HashIDSet();
            for (int iTerm = 0; iTerm < block.size(); iTerm++) {
                int termId = block.get(iTerm).getAsJsonObject().get("id").getAsInt();
                blockTerms.add(termId);
            }
            terms.add(blockTerms);
        }
        return terms;
    }
    
    public List<MutableIdentifiableIDSet> readAll(
            File inputDir,
            boolean firstBlockOnly
    ) throws java.io.IOException {

        List<MutableIdentifiableIDSet> domains = new ArrayList<>();
        
        for (File file : inputDir.listFiles()) {
            if (file.getName().endsWith(".json")) {
                domains.add(this.read(file, firstBlockOnly));
            }
        }
        
        return domains;
    }
    
    public List<List<IDSet>> readAllBlocks(File inputDir) throws java.io.IOException {

        List<List<IDSet>> domains = new ArrayList<>();
        
        for (File file : inputDir.listFiles()) {
            if (file.getName().endsWith(".json")) {
                domains.add(this.readBlocks(file));
            }
        }
        
        return domains;
    }
}
