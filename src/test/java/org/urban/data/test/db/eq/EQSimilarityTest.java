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
package org.urban.data.test.db.eq;

import java.math.BigDecimal;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.opendata.db.eq.similarity.JISimilarityArray;

/**
 *
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class EQSimilarityTest {
    
    public EQSimilarityTest() {
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
    public void testJISimilarityArray() {
    
        JISimilarityArray simFunc = new JISimilarityArray()
                .add(0, new Integer[]{1, 2, 3, 4 ,5})
                .add(1, new Integer[]{1, 3, 5, 6})
                .add(2, new Integer[]{3, 5, 7})
                .add(3, new Integer[]{10, 11, 12});
        
        assertEquals(new BigDecimal((double)2/(double)6), simFunc.sim(0, 1));
        assertEquals(new BigDecimal((double)2/(double)6), simFunc.sim(0, 2));
        assertEquals(new BigDecimal((double)2/(double)5), simFunc.sim(1, 2));
        assertEquals(BigDecimal.ZERO, simFunc.sim(0, 3));
    }
}
