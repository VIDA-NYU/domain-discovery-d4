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
package org.opendata.curation.d4.signature.trim;

import java.math.BigDecimal;
import java.util.List;
import org.opendata.curation.d4.signature.ContextSignatureBlock;
import org.opendata.curation.d4.signature.ContextSignatureBlocksConsumer;

/**
 * Robustifier for context signatures that retains all but the last block. If
 * the signature only contains a single block that block is retained and no
 * pruning occurs.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IgnoreLastBlockRobustifier extends SignatureRobustifier {

    public IgnoreLastBlockRobustifier(ContextSignatureBlocksConsumer consumer) {
        
        super(consumer);
    }

    @Override
    public void consume(int nodeId, BigDecimal sim, List<ContextSignatureBlock> blocks) {

        this.push(nodeId, sim, blocks, Math.max(1, blocks.size() - 1));
    }
}
