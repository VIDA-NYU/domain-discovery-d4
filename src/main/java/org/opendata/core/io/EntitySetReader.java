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
package org.opendata.core.io;

import java.io.BufferedReader;
import java.io.File;
import org.opendata.core.object.Entity;
import org.opendata.core.object.filter.AnyObjectFilter;
import org.opendata.core.object.filter.ObjectFilter;
import org.opendata.core.set.EntitySet;

/**
 * Read an entity set file. Assumes a text file where each row has at least two
 * tab-delimited columns containing the unique entity identifier and the entity
 * name. Additional columns in each row are ignored.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EntitySetReader {
    
    private final File _file;
    
    public EntitySetReader(File file) {
        
        _file = file;
    }
    
    public void read(
            ObjectFilter<Integer> filter,
            EntityConsumer consumer
    ) throws java.io.IOException {
        
        consumer.open();
        
        try (BufferedReader in = FileSystem.openReader(_file)) {
	    String line;
	    while ((line = in.readLine()) != null) {
		String[] tokens = line.split("\t");
                int id = Integer.parseInt(tokens[0]);
                if (filter.contains(id)) {
                    consumer.consume(new Entity(id, tokens[1]));
                }
            }
        }
        
        consumer.close();
    }
    
    public void read(EntityConsumer consumer) throws java.io.IOException {
        
        this.read(new AnyObjectFilter(), consumer);
    }
    
    public EntitySet readEntities(ObjectFilter<Integer> filter) throws java.io.IOException {
        
        EntitySet result = new EntitySet();
        
        this.read(filter, new EntityBuffer(result));
        
        return result;
    }

}
