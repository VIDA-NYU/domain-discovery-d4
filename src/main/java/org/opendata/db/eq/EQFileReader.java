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

import org.opendata.core.set.SortedIDList;
import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.SortedIDArray;
import org.opendata.core.util.StringHelper;

/**
 * Read a set of equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQFileReader extends CompressedDatabase {
   
    private final File _file;
    
    public EQFileReader(File file) {
        
        _file = file;
    }
    
    @Override
    public HashMap<Integer, SortedIDList> getEQColumns() {
        
        HashMap<Integer, SortedIDList> result = new HashMap<>();
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                int eqId = Integer.parseInt(tokens[0]);
                String[] cols = tokens[2].split(",");
                Integer[] columns = new Integer[cols.length];
                for (int iCol = 0; iCol < cols.length; iCol++) {
                    String c = cols[iCol];
                    columns[iCol] = Integer.parseInt(c.substring(0, c.indexOf(":")));
                }
                result.put(eqId, new SortedIDArray(columns));
            }
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
        
        return result;
    }

    @Override
    public List<Integer> getEQIdentifiers() {

        List<Integer> result = new ArrayList<>();
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
            String line;
            while ((line = in.readLine()) != null) {
                int eqId = Integer.parseInt(line.substring(0, line.indexOf("\t")));
                result.add(eqId);
            }
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
        
        return result;
    }
    
    @Override
    public Integer[] getEQTermCounts() {
        
        HashMap<Integer, Integer> counts = new HashMap<>();
        int maxId = -1;
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                int eqId = Integer.parseInt(tokens[0]);
                counts.put(eqId, StringHelper.splitSize(tokens[1], ','));
                if (eqId > maxId) {
                    maxId = eqId;
                }
            }
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
        
        Integer[] result = new Integer[maxId + 1];
        for (Integer key : counts.keySet()) {
            result[key] = counts.get(key);
        }
        return result;
    }
}
