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
package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

@Internal
public class ChangelogDataIterator extends DataIterator<RowData> {

  private final ChangelogScanTaskReader changelogReader;
  private final List<ChangelogScanTask> changelogTasks;
  private final InputFilesDecryptor inputFilesDecryptor;

  private int taskIndex;
  private CloseableIterator<RowData> currentIterator;
  private int fileOffset;
  private long recordOffset;

  public ChangelogDataIterator(
      ChangelogScanTaskReader changelogReader,
      List<ChangelogScanTask> changelogTasks,
      FileIO io,
      EncryptionManager encryption) {
    super();
    this.changelogReader = changelogReader;
    this.changelogTasks = changelogTasks;
    this.inputFilesDecryptor = buildDecryptor(changelogTasks, io, encryption);
    this.taskIndex = 0;
    this.currentIterator = CloseableIterator.empty();
    this.fileOffset = -1;
    this.recordOffset = 0L;
  }

  @Override
  public void seek(int startingFileOffset, long startingRecordOffset) {
    Preconditions.checkState(
        fileOffset == -1, "Seek should be called before any other iterator actions");
    Preconditions.checkState(
        startingFileOffset < changelogTasks.size(),
        "Invalid starting file offset %s for changelog tasks with %s tasks",
        startingFileOffset,
        changelogTasks.size());

    taskIndex = startingFileOffset;
    updateCurrentIterator();

    for (long i = 0; i < startingRecordOffset; ++i) {
      if (currentFileHasNext() && hasNext()) {
        next();
      } else {
        throw new IllegalStateException(
            String.format(
                Locale.ROOT,
                "Invalid starting record offset %d for task %d",
                startingRecordOffset,
                startingFileOffset));
      }
    }

    fileOffset = startingFileOffset;
    recordOffset = startingRecordOffset;
  }

  @Override
  public boolean hasNext() {
    updateCurrentIterator();
    return currentIterator.hasNext();
  }

  @Override
  public RowData next() {
    updateCurrentIterator();
    recordOffset += 1;
    return currentIterator.next();
  }

  @Override
  public boolean currentFileHasNext() {
    return currentIterator.hasNext();
  }

  @Override
  public int fileOffset() {
    return fileOffset;
  }

  @Override
  public long recordOffset() {
    return recordOffset;
  }

  @Override
  public void close() throws IOException {
    currentIterator.close();
  }

  private void updateCurrentIterator() {
    try {
      while (!currentIterator.hasNext() && taskIndex < changelogTasks.size()) {
        currentIterator.close();
        currentIterator =
            changelogReader.open(changelogTasks.get(taskIndex), inputFilesDecryptor);
        taskIndex++;
        fileOffset += 1;
        recordOffset = 0L;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static InputFilesDecryptor buildDecryptor(
      List<ChangelogScanTask> tasks, FileIO io, EncryptionManager encryption) {
    Map<String, ByteBuffer> keyMetadata = Maps.newHashMap();

    for (ChangelogScanTask task : tasks) {
      if (task instanceof AddedRowsScanTask) {
        AddedRowsScanTask addedTask = (AddedRowsScanTask) task;
        addFileKeyMetadata(keyMetadata, addedTask.file());
        for (DeleteFile deleteFile : addedTask.deletes()) {
          addFileKeyMetadata(keyMetadata, deleteFile);
        }
      } else if (task instanceof DeletedDataFileScanTask) {
        DeletedDataFileScanTask deletedTask = (DeletedDataFileScanTask) task;
        addFileKeyMetadata(keyMetadata, deletedTask.file());
        for (DeleteFile deleteFile : deletedTask.existingDeletes()) {
          addFileKeyMetadata(keyMetadata, deleteFile);
        }
      }
    }

    Stream<EncryptedInputFile> encrypted =
        keyMetadata.entrySet().stream()
            .map(
                entry ->
                    EncryptedFiles.encryptedInput(
                        io.newInputFile(entry.getKey()), entry.getValue()));

    @SuppressWarnings("StreamToIterable")
    Iterable<InputFile> decryptedFiles = encryption.decrypt(encrypted::iterator);

    Map<String, InputFile> files = Maps.newHashMapWithExpectedSize(keyMetadata.size());
    decryptedFiles.forEach(decrypted -> files.putIfAbsent(decrypted.location(), decrypted));

    return new InputFilesDecryptor(files);
  }

  private static <F extends ContentFile<F>> void addFileKeyMetadata(
      Map<String, ByteBuffer> keyMetadata, ContentFile<F> file) {
    keyMetadata.put(file.location(), file.keyMetadata());
  }
}
