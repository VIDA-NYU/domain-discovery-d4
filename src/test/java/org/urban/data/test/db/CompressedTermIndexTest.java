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
package org.urban.data.test.db;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.opendata.core.object.IdentifiableInteger;
import org.opendata.core.set.SortedIDSet;
import org.opendata.core.util.StringHelper;
import org.opendata.core.util.count.IdentifiableCount;
import org.opendata.db.column.ColumnHelper;
import org.opendata.db.eq.CompressedTermIndexGenerator;
import org.opendata.db.eq.EQWriter;
import org.opendata.db.term.Term;

/**
 * Unit tests for generating compressed term indexes.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CompressedTermIndexTest {
    
    private class EQBuffer  implements EQWriter {
    
        private final HashMap<String, String> _buffer = new HashMap<>();

        public String get(String key) {
            
            return _buffer.get(key);
        }
        
        @Override
        public <T extends IdentifiableInteger> void write(List<Integer> terms, SortedIDSet<T> columns) {
            
            Collections.sort(terms);
            
            _buffer.put(StringHelper.joinIntegers(terms), ColumnHelper.toArrayString(columns));
        }
        
        public int size() {
            
            return _buffer.size();
        }
    }
    
    public CompressedTermIndexTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testEQGenerator() {
    
        EQBuffer buf = new EQBuffer();
        
        CompressedTermIndexGenerator consumer;
        consumer = new CompressedTermIndexGenerator(buf, false);
        
        Term term1 = new Term(
                0,
                "A",
                new SortedIDSet<>(new IdentifiableInteger[]{
                    new IdentifiableCount(0, 1),
                    new IdentifiableCount(1, 2)
                })
        );
        Term term2 = new Term(
                1,
                "B",
                new SortedIDSet<>(new IdentifiableInteger[]{
                    new IdentifiableCount(0, 1),
                    new IdentifiableCount(2, 2)
                })
        );
        Term term3 = new Term(
                2,
                "C",
                new SortedIDSet<>(new IdentifiableInteger[]{
                    new IdentifiableCount(0, 2),
                    new IdentifiableCount(1, 4)
                })
        );

        consumer.open();
        consumer.consume(term1);
        consumer.consume(term2);
        consumer.consume(term3);
        consumer.close();
        
        assertEquals(buf.size(), 2);
        assertEquals(buf.get("0,2"), "0:3,1:6");
        assertEquals(buf.get("1"), "0:1,2:2");
    }
}
