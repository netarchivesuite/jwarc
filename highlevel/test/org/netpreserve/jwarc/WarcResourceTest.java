/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (C) 2018 National Library of Australia and the jwarc contributors
 */

package org.netpreserve.jwarc;

import org.junit.Test;

import static org.junit.Assert.*;

public class WarcResourceTest {
    @Test
    public void builder() {
        WarcResource resource = new WarcResource.Builder().build();
        assertEquals("resource", resource.type());
    }

}