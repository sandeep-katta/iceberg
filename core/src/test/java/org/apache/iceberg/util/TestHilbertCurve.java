/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.primitives.UnsignedBytes;
import org.junit.jupiter.api.Test;

public class TestHilbertCurve {

  private static final int NUM_TESTS = 10_000;
  private static final Comparator<byte[]> BYTES_COMPARATOR =
      UnsignedBytes.lexicographicalComparator();

  private final Random random = new Random(42);

  // -------------------------------------------------------------------------
  // bytesToUnsignedLong
  // -------------------------------------------------------------------------

  @Test
  public void testBytesToUnsignedLongSingleByte() {
    assertThat(HilbertCurve.bytesToUnsignedLong(new byte[] {0x01}))
        .isEqualTo(0x0100000000000000L);
  }

  @Test
  public void testBytesToUnsignedLongFullLong() {
    byte[] bytes = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    assertThat(HilbertCurve.bytesToUnsignedLong(bytes)).isEqualTo(0x0102030405060708L);
  }

  @Test
  public void testBytesToUnsignedLongTruncatesAt8() {
    byte[] bytes = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, (byte) 0xFF};
    // Extra byte is ignored
    assertThat(HilbertCurve.bytesToUnsignedLong(bytes)).isEqualTo(0x0102030405060708L);
  }

  @Test
  public void testBytesToUnsignedLongZeroBytes() {
    assertThat(HilbertCurve.bytesToUnsignedLong(new byte[] {0x00})).isEqualTo(0L);
  }

  @Test
  public void testBytesToUnsignedLongPreservesOrder() {
    // Smaller value → smaller long
    long a = HilbertCurve.bytesToUnsignedLong(new byte[] {0x01, 0x00});
    long b = HilbertCurve.bytesToUnsignedLong(new byte[] {0x02, 0x00});
    assertThat(Long.compareUnsigned(a, b)).isLessThan(0);
  }

  // -------------------------------------------------------------------------
  // toIndex — basic properties
  // -------------------------------------------------------------------------

  @Test
  public void testToIndexRejectsNullColumns() {
    assertThatThrownBy(() -> HilbertCurve.toIndex(null, 8))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("no columns");
  }

  @Test
  public void testToIndexRejectsEmptyColumns() {
    assertThatThrownBy(() -> HilbertCurve.toIndex(new byte[0][], 8))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("no columns");
  }

  @Test
  public void testToIndexRejectsZeroOutputSize() {
    assertThatThrownBy(
            () -> HilbertCurve.toIndex(new byte[][] {{0x01}}, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Output size must be positive");
  }

  @Test
  public void testToIndexOutputLength() {
    byte[] col = longToBytes(42L);
    assertThat(HilbertCurve.toIndex(new byte[][] {col}, 8)).hasSize(8);
    assertThat(HilbertCurve.toIndex(new byte[][] {col}, 16)).hasSize(16);
  }

  @Test
  public void testToIndexDeterministic() {
    byte[] col = longToBytes(123456789L);
    byte[] a = HilbertCurve.toIndex(new byte[][] {col}, 8);
    byte[] b = HilbertCurve.toIndex(new byte[][] {col}, 8);
    assertThat(a).isEqualTo(b);
  }

  @Test
  public void testToIndexAllZeros() {
    byte[] zero = new byte[8];
    byte[] result = HilbertCurve.toIndex(new byte[][] {zero, zero}, 16);
    assertThat(result).isEqualTo(new byte[16]);
  }

  // -------------------------------------------------------------------------
  // Locality: 1D — monotone ordering
  // -------------------------------------------------------------------------

  @Test
  public void testOneDimensionalMonotone() {
    // For a single dimension, the Hilbert "curve" should produce indices that
    // are monotonically ordered (since there is only one axis).
    int outputSize = 8;
    byte[] prev = null;
    for (int i = 0; i < 256; i++) {
      byte[] col = longToBytes(i);
      byte[] h = HilbertCurve.toIndex(new byte[][] {col}, outputSize);
      if (prev != null) {
        assertThat(BYTES_COMPARATOR.compare(h, prev))
            .as("1D Hilbert index should be non-decreasing for i=%d", i)
            .isGreaterThanOrEqualTo(0);
      }
      prev = h;
    }
  }

  // -------------------------------------------------------------------------
  // 2D: distinct inputs produce distinct indices
  // -------------------------------------------------------------------------

  @Test
  public void testTwoDimensionalDistinctInputs() {
    // Verify that different 2D inputs map to different Hilbert indices
    int outputSize = 16;
    int found = 0;
    for (int x = 0; x < 16; x++) {
      for (int y = 0; y < 16; y++) {
        byte[] h1 = hilbert2D(x, y, outputSize);
        byte[] h2 = hilbert2D(x + 1, y, outputSize);
        if (!Arrays.equals(h1, h2)) {
          found++;
        }
      }
    }
    // Most adjacent pairs should differ
    assertThat(found).isGreaterThan(100);
  }

  @Test
  public void testTwoDimensionalOriginVsNonOrigin() {
    // (0,0) and (0,1) must produce different Hilbert indices
    byte[] h00 = hilbert2D(0L, 0L, 16);
    byte[] h01 = hilbert2D(0L, 1L, 16);
    byte[] h10 = hilbert2D(1L, 0L, 16);
    assertThat(h00).isNotEqualTo(h01);
    assertThat(h00).isNotEqualTo(h10);
  }

  // -------------------------------------------------------------------------
  // Consistency: same result regardless of input array length (truncation)
  // -------------------------------------------------------------------------

  @Test
  public void testTruncationConsistency() {
    byte[] eightBytes = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    byte[] nineBytes = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, (byte) 0xFF};

    byte[] a = HilbertCurve.toIndex(new byte[][] {eightBytes}, 8);
    byte[] b = HilbertCurve.toIndex(new byte[][] {nineBytes}, 8);

    // The 9th byte is ignored — results should be identical
    assertThat(a).isEqualTo(b);
  }

  // -------------------------------------------------------------------------
  // Consistency: ZOrderByteUtils converters work as inputs
  // -------------------------------------------------------------------------

  @Test
  public void testWithZOrderByteUtilsConverters() {
    // Verify that ZOrderByteUtils-produced byte arrays feed correctly into Hilbert
    ByteBuffer buf = ByteBuffer.allocate(8);
    byte[] col1 = ZOrderByteUtils.intToOrderedBytes(100, buf).array().clone();
    byte[] col2 = ZOrderByteUtils.intToOrderedBytes(200, buf).array().clone();

    byte[] h1 = HilbertCurve.toIndex(new byte[][] {col1}, 8);
    byte[] h2 = HilbertCurve.toIndex(new byte[][] {col2}, 8);

    // Different inputs → different Hilbert indices
    assertThat(h1).isNotEqualTo(h2);
  }

  // -------------------------------------------------------------------------
  // Multi-dimensional: dimension count affects output
  // -------------------------------------------------------------------------

  @Test
  public void testDifferentDimensionCounts() {
    byte[] col = longToBytes(42L);

    byte[] h1d = HilbertCurve.toIndex(new byte[][] {col}, 8);
    byte[] h2d = HilbertCurve.toIndex(new byte[][] {col, col}, 8);
    byte[] h3d = HilbertCurve.toIndex(new byte[][] {col, col, col}, 8);

    // Different dimension counts produce different indices for the same value
    assertThat(h1d).isNotEqualTo(h2d);
    assertThat(h1d).isNotEqualTo(h3d);
  }

  // -------------------------------------------------------------------------
  // Random stress test: no exceptions, output is always correct size
  // -------------------------------------------------------------------------

  @Test
  public void testRandomInputsNeverThrow() {
    for (int i = 0; i < NUM_TESTS; i++) {
      int n = random.nextInt(4) + 1;
      byte[][] cols = new byte[n][];
      for (int d = 0; d < n; d++) {
        cols[d] = longToBytes(random.nextLong());
      }
      int outputSize = random.nextInt(16) + 1;
      byte[] result = HilbertCurve.toIndex(cols, outputSize);
      assertThat(result).hasSize(outputSize);
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private byte[] longToBytes(long val) {
    return ByteBuffer.allocate(8).putLong(val).array();
  }

  private byte[] hilbert2D(long x, long y, int outputSize) {
    return HilbertCurve.toIndex(new byte[][] {longToBytes(x), longToBytes(y)}, outputSize);
  }

  private long toLong(byte[] bytes) {
    long result = 0;
    for (int i = 0; i < Math.min(bytes.length, 8); i++) {
      result = (result << 8) | (bytes[i] & 0xFFL);
    }
    return result;
  }
}
