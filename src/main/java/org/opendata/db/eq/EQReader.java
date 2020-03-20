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

import java.io.BufferedReader;
import java.io.File;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.HashObjectSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.set.ImmutableIDSet;

/**
 * Read a set of equivalence classes.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQReader implements EQStream {
   
    private final File _file;
    
    public EQReader(File file) {
        
        _file = file;
    }
    
    private EQ parseLine(String line) throws java.lang.IllegalArgumentException {
        
        String[] tokens = line.split("\t");
        switch (tokens.length) {
            case 3:
                return new EQImpl(
                        Integer.parseInt(tokens[0]),
                        new ImmutableIDSet(tokens[1]),
                        parseColumnList(tokens[2])
                );
            case 4:
                return new EQImpl(
                        Integer.parseInt(tokens[0]),
                        new ImmutableIDSet(tokens[2]),
                        Integer.parseInt(tokens[1]),
                        parseColumnList(tokens[3])
                );
            default:
                throw new java.lang.IllegalArgumentException(line);
        }
    }
    
    private IDSet parseColumnList(String list) {
    
        HashIDSet columns = new HashIDSet();
        
        for (String token : list.split(",")) {
            if (token.contains(":")) {
                columns.add(Integer.parseInt(token.substring(0, token.indexOf(":"))));
            } else {
                columns.add(Integer.parseInt(token));
            }
        }
        return columns;
    }
        
    public IdentifiableObjectSet<EQ> read() throws java.io.IOException {
        
        HashObjectSet<EQ> result = new HashObjectSet<>();
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (!line.equals("")) {
                    result.add(this.parseLine(line));
                }
            }
        }
        
        return result;
    }

    @Override
    public void stream(EQConsumer consumer) {

        consumer.open();
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (!line.equals("")) {
                    consumer.consume(this.parseLine(line));
                }
            }
        } catch (java.io.IOException ex) {
            throw new RuntimeException(ex);
        }
        
        consumer.close();
    }
}
