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
package org.opendata.test.d4.signature;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.opendata.curation.d4.signature.MultiBlockSignatureIterator;
import org.opendata.curation.d4.signature.SignatureBlock;
import org.opendata.curation.d4.signature.SignatureBlockImpl;

/**
 * Unit tests for robust signature iterators.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class RobustSignatureIteratorTest {
    
    public RobustSignatureIteratorTest() {
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
    public void testMultiBlockSignatureIterator() {
        
        List<SignatureBlock> blocks = new ArrayList<>();
        assertFalse(new MultiBlockSignatureIterator(blocks).hasNext());
        
        blocks.add(new SignatureBlockImpl(new Integer[]{1}, 1));
        assertArrayEquals(
                new Integer[]{1},
                toArray(new MultiBlockSignatureIterator(blocks))
        );
        
        blocks.add(new SignatureBlockImpl(new Integer[]{2, 3, 4}, 1));
        assertArrayEquals(
                new Integer[]{1, 2, 3, 4},
                toArray(new MultiBlockSignatureIterator(blocks))
        );
        
        blocks.add(new SignatureBlockImpl(new Integer[]{5}, 1));
        assertArrayEquals(
                new Integer[]{1, 2, 3, 4, 5},
                toArray(new MultiBlockSignatureIterator(blocks))
        );
    }
    
    private static Integer[] toArray(Iterator<Integer> iterator) {
        
        List<Integer> elements = new ArrayList<>();
        while (iterator.hasNext()) {
            elements.add(iterator.next());
        }
        
        Integer[] result = new Integer[elements.size()];
        for (int iEl = 0; iEl < elements.size(); iEl++) {
            result[iEl] = elements.get(iEl);
        }
        return result;
    }
}
