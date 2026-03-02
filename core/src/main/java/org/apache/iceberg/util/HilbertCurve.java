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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Computes N-dimensional Hilbert curve indices for liquid clustering.
 *
 * <p>Implements the algorithm from: J. Skilling, "Programming the Hilbert Curve," AIP Conference
 * Proceedings 707, pp. 381-387 (2004).
 *
 * <p>The Hilbert curve provides better spatial locality than Z-order (Morton code) because it
 * never has the discontinuities that occur when Z-order crosses quadrant boundaries. This leads to
 * better data clustering and faster range queries.
 *
 * <p>The input byte arrays use the same ordered byte representations as {@link ZOrderByteUtils} —
 * type-conversion UDFs are fully shared between the two curve implementations.
 */
public class HilbertCurve {

  /** Number of bits used per dimension (columns map to 64-bit unsigned coordinates). */
  static final int BITS_PER_DIM = 64;

  private HilbertCurve() {}

  /**
   * Computes the Hilbert curve index for N-dimensional column values.
   *
   * <p>Each column's byte array is interpreted as a big-endian unsigned integer. Arrays shorter
   * than 8 bytes are left-aligned (high bits filled), preserving order. Arrays longer than 8 bytes
   * are truncated to the first 8 bytes.
   *
   * @param columnsBinary ordered byte representations (one per column, as produced by {@link
   *     ZOrderByteUtils})
   * @param outputSize number of output bytes to produce (taken from the MSB end of the full index)
   * @return Hilbert index as a big-endian byte array of length {@code outputSize}
   */
  public static byte[] toIndex(byte[][] columnsBinary, int outputSize) {
    Preconditions.checkArgument(
        columnsBinary != null && columnsBinary.length > 0,
        "Cannot compute Hilbert index with no columns");
    Preconditions.checkArgument(outputSize > 0, "Output size must be positive");

    int n = columnsBinary.length;

    // Convert each column's byte array to a 64-bit unsigned coordinate
    long[] coords = new long[n];
    for (int i = 0; i < n; i++) {
      coords[i] = bytesToUnsignedLong(columnsBinary[i]);
    }

    // Apply Skilling's AxestoTranspose transform in-place
    axesToTranspose(coords);

    // Pack the transposed bits into output bytes (MSB first)
    return packBits(coords, outputSize);
  }

  /**
   * Skilling (2004) AxestoTranspose: converts N-dimensional Cartesian coordinates in-place to the
   * "transposed" representation of the Hilbert index.
   *
   * <p>After this transform, the k-th bit of the Hilbert index (counting from the MSB, k=0)
   * resides at {@code X[k % n]} bit position {@code BITS_PER_DIM - 1 - k/n}.
   */
  static void axesToTranspose(long[] X) {
    int n = X.length;
    long M = 1L << (BITS_PER_DIM - 1);

    // Inverse undo: peel off the Gray-code and apply the recursive Hilbert structure
    for (long Q = M; Q > 1L; Q >>>= 1) {
      long P = Q - 1L;
      for (int i = 0; i < n; i++) {
        if ((X[i] & Q) != 0L) {
          X[0] ^= P; // invert
        } else {
          long t = (X[0] ^ X[i]) & P;
          X[0] ^= t;
          X[i] ^= t; // swap
        }
      }
    }

    // Gray encode the transposed result
    for (int i = 1; i < n; i++) {
      X[i] ^= X[i - 1];
    }
    long t = 0L;
    for (long Q = M; Q > 1L; Q >>>= 1) {
      if ((X[n - 1] & Q) != 0L) {
        t ^= Q - 1L;
      }
    }
    for (int i = 0; i < n; i++) {
      X[i] ^= t;
    }
  }

  /**
   * Packs the transposed Hilbert coordinates into an output byte array (big-endian, MSB first).
   *
   * <p>In the transposed form, the k-th output bit (k=0 is the MSB of {@code output[0]}) comes
   * from {@code X[k % n]} at bit position {@code BITS_PER_DIM - 1 - k/n}. We emit {@code
   * outputSize * 8} bits, or the full index length {@code n * BITS_PER_DIM}, whichever is smaller.
   */
  static byte[] packBits(long[] X, int outputSize) {
    int n = X.length;
    byte[] output = new byte[outputSize];
    int maxBits = Math.min(outputSize * 8, n * BITS_PER_DIM);

    for (int k = 0; k < maxBits; k++) {
      int dim = k % n;
      int bitPos = BITS_PER_DIM - 1 - k / n;
      int bit = (int) ((X[dim] >>> bitPos) & 1L);
      if (bit == 1) {
        // k/8 is the output byte index; 0x80 >>> (k%8) picks the right bit within the byte
        output[k >>> 3] |= (byte) (0x80 >>> (k & 7));
      }
    }

    return output;
  }

  /**
   * Converts a byte array to an unsigned 64-bit coordinate (big-endian, left-aligned).
   *
   * <p>If the array is shorter than 8 bytes, the value occupies the high bits (equivalent to
   * zero-padding on the right), which preserves sort order. If the array is longer than 8 bytes,
   * only the first 8 bytes are used.
   */
  static long bytesToUnsignedLong(byte[] bytes) {
    long result = 0L;
    int len = Math.min(bytes.length, 8);
    for (int i = 0; i < len; i++) {
      result = (result << 8) | (bytes[i] & 0xFFL);
    }
    // Left-align: shift value to occupy the most significant bits
    if (len < 8) {
      result <<= 8 * (8 - len);
    }
    return result;
  }
}
