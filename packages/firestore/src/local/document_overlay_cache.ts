/**
 * @license
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { DocumentKey } from '../model/document_key';
import { Overlay } from '../model/overlay';
import { Mutation } from '../model/mutation';
import { ResourcePath } from '../model/path';

/**
 * Provides methods to read and write document overlays.
 *
 * <p>An overlay is a saved mutation, that gives a local view of a document when
 * applied to the remote version of the document.
 *
 * <p>Each overlay stores the largest batch ID that is included in the overlay,
 * which allows us to remove the overlay once all batches leading up to it have
 * been acknowledged.
 */
export interface DocumentOverlayCache {
  /**
   * Gets the saved overlay mutation for the given document key.
   * Returns null if there is no overlay for that key.
   */
  getOverlay(key: DocumentKey): Overlay | null;

  /**
   * Saves the given document key to mutation map to persistence as overlays.
   * All overlays will have their largest batch id set to `largestBatchId`.
   */
  saveOverlays(
    largestBatchId: number,
    overlays: Map<DocumentKey, Mutation>
  ): void;

  /** Removes the overlay whose largest-batch-id equals to the given Id. */
  removeOverlaysForBatchId(batchId: number): void;

  /**
   * Returns all saved overlays for the given collection.
   *
   * @param collection The collection path to get the overlays for.
   * @param sinceBatchId The minimum batch ID to filter by (exclusive).
   * Only overlays that contain a change past `sinceBatchId` are returned.
   * @returns Mapping of each document key in the collection to its overlay.
   */
  getOverlaysForCollection(
    collection: ResourcePath,
    sinceBatchId: number
  ): Map<DocumentKey, Overlay>;

  /**
   * Returns `count` overlays with a batch ID higher than `sinceBatchId` for the
   * provided collection group, processed by ascending batch ID. The method
   * always returns all overlays for a batch even if the last batch contains
   * more documents than the remaining limit.
   *
   * @param collectionGroup The collection group to get the overlays for.
   * @param sinceBatchId The minimum batch ID to filter by (exclusive).
   * Only overlays that contain a change past `sinceBatchId` are returned.
   * @param count The number of overlays to return. Can be exceeded if the last
   * batch contains more entries.
   * @return Mapping of each document key in the collection group to its overlay.
   */
  getOverlaysForCollectionGroup(
    collectionGroup: string,
    sinceBatchId: number,
    count: number
  ): Map<DocumentKey, Overlay>;
}
