/**
 * @license
 * Copyright 2017 Google LLC
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

import {
  asCollectionQueryAtPath,
  isCollectionGroupQuery,
  isDocumentQuery,
  Query,
  queryMatches
} from '../core/query';
import {
  DocumentKeySet,
  documentKeySet,
  DocumentMap,
  documentMap,
  mutableDocumentMap,
  MutableDocumentMap
} from '../model/collections';
import { Document, MutableDocument } from '../model/document';
import { DocumentKey } from '../model/document_key';
import {
  calculateOverlayMutation,
  Mutation,
  mutationApplyToLocalView,
  PatchMutation
} from '../model/mutation';
import { ResourcePath } from '../model/path';
import { debugAssert } from '../util/assert';

import { IndexManager } from './index_manager';
import { MutationQueue } from './mutation_queue';
import { PersistencePromise } from './persistence_promise';
import { PersistenceTransaction } from './persistence_transaction';
import { RemoteDocumentCache } from './remote_document_cache';
import { DocumentOverlayCache } from './document_overlay_cache';
import { Overlay } from '../model/overlay';
import { Timestamp } from '../api';
import { SortedMap } from '../util/sorted_map';
import { FieldMask } from '../model/field_mask';
import { IndexOffset } from '../model/field_index';

/**
 * A readonly view of the local state of all documents we're tracking (i.e. we
 * have a cached version in remoteDocumentCache or local mutations for the
 * document). The view is computed by applying the mutations in the
 * MutationQueue to the RemoteDocumentCache.
 */
export class LocalDocumentsView {
  constructor(
    readonly remoteDocumentCache: RemoteDocumentCache,
    readonly mutationQueue: MutationQueue,
    readonly documentOverlayCache: DocumentOverlayCache,
    readonly indexManager: IndexManager
  ) {}

  /**
   * Get the local view of the document identified by `key`.
   *
   * @returns Local view of the document or null if we don't have any cached
   * state for it.
   */
  getDocument(
    transaction: PersistenceTransaction,
    key: DocumentKey
  ): PersistencePromise<Document> {
    let overlay: Overlay | null = null;
    return this.documentOverlayCache
      .getOverlay(transaction, key)
      .next(value => {
        overlay = value;
        return this.getBaseDocument(transaction, key, overlay);
      })
      .next(document => {
        if (overlay !== null) {
          mutationApplyToLocalView(
            overlay.mutation,
            document,
            null,
            Timestamp.now()
          );
        }
        return document as Document;
      });
  }

  /**
   * Gets the local view of the documents identified by `keys`.
   *
   * If we don't have cached state for a document in `keys`, a NoDocument will
   * be stored for that key in the resulting set.
   */
  getDocuments(
    transaction: PersistenceTransaction,
    keys: DocumentKeySet
  ): PersistencePromise<DocumentMap> {
    return this.remoteDocumentCache
      .getEntries(transaction, keys)
      .next(docs =>
        this.getLocalViewOfDocuments(
          transaction,
          docs,
          new Set<DocumentKey>()
        ).next(() => docs as DocumentMap)
      );
  }

  /**
   * Similar to `getDocuments`, but creates the local view from the given
   * `baseDocs` without retrieving documents from the local store.
   *
   * @param transaction - The transaction this operation is scoped to.
   * @param docs - The documents to apply local mutations to get the local views.
   * @param existenceStateChanged - The set of document keys whose existence state
   *   is changed. This is useful to determine if some documents overlay needs
   *   to be recalculated.
   */
  getLocalViewOfDocuments(
    transaction: PersistenceTransaction,
    docs: MutableDocumentMap,
    existenceStateChanged: Set<DocumentKey>
  ): PersistencePromise<DocumentMap> {
    return this.computeViews(
      transaction,
      docs,
      new Map<DocumentKey, Overlay>(),
      existenceStateChanged
    );
  }

  /**
   * Computes the local view for documents, applying overlays from both
   * `memoizedOverlays` and the overlay cache.
   */
  computeViews(
    transaction: PersistenceTransaction,
    docs: MutableDocumentMap,
    memoizedOverlays: Map<DocumentKey, Overlay>,
    existenceStateChanged: Set<DocumentKey>
  ): PersistencePromise<DocumentMap> {
    let results = documentMap();
    let recalculateDocuments = mutableDocumentMap();
    const promises: Array<PersistencePromise<void>> = [];
    docs.forEach((_, doc) => {
      const overlayPromise = memoizedOverlays.has(doc.key)
        ? PersistencePromise.resolve(memoizedOverlays.get(doc.key)!)
        : this.documentOverlayCache.getOverlay(transaction, doc.key);

      promises.push(
        overlayPromise.next(overlay => {
          // Recalculate an overlay if the document's existence state is changed
          // due to a remote event *and* the overlay is a PatchMutation. This is
          // because document existence state can change if some patch mutation's
          // preconditions are met.
          // NOTE: we recalculate when `overlay` is null as well, because there
          // might be a patch mutation whose precondition does not match before
          // the change (hence overlay==null), but would now match.
          if (
            existenceStateChanged.has(doc.key) &&
            (overlay == null || overlay.mutation instanceof PatchMutation)
          ) {
            recalculateDocuments.insert(doc.key, doc);
          } else if (overlay != null) {
            mutationApplyToLocalView(
              overlay.mutation,
              doc,
              null,
              Timestamp.now()
            );
          }
        })
      );
    });

    return PersistencePromise.waitFor(promises)
      .next(() =>
        this.recalculateAndSaveOverlays(transaction, recalculateDocuments)
      )
      .next(() => {
        docs.forEach((key, value) => results.insert(key, value));
        return results;
      });
  }

  private recalculateAndSaveOverlays(
    transaction: PersistenceTransaction,
    docs: MutableDocumentMap
  ): PersistencePromise<void> {
    let masks = new Map<DocumentKey, FieldMask>();
    // A reverse lookup map from batch id to the documents within that batch.
    let documentsByBatchId = new SortedMap<number, Set<DocumentKey>>(
      (key1: number, key2: number) => key1 - key2
    );
    let processed = new Set<DocumentKey>();
    return this.mutationQueue
      .getAllMutationBatchesAffectingDocumentKeys(transaction, docs)
      .next(batches => {
        batches.forEach(batch => {
          batch.keys().forEach(key => {
            // TODO(ehsann): Android uses a Set for FieldMask fields, but Web
            //  uses an array. Should we change it?
            let mask = masks.has(key) ? masks.get(key)! : new FieldMask([]);
            // TODO(ehsann): mask can be legitimately null...
            mask = batch.applyToLocalViewWithFieldMask(docs.get(key)!, mask)!;
            masks.set(key, mask);
            if (documentsByBatchId.get(batch.batchId) === null) {
              documentsByBatchId.insert(batch.batchId, new Set<DocumentKey>());
            }
            documentsByBatchId.get(batch.batchId)!.add(key);
          });
        });
      })
      .next(() => {
        const promises: Array<PersistencePromise<void>> = [];
        // Iterate in descending order of batch IDs, and skip documents that are
        // already saved.
        let iter = documentsByBatchId.getReverseIterator();
        while (iter.hasNext()) {
          const entry = iter.getNext();
          const batchId = entry.key;
          const keys = entry.value;
          let overlays = new Map<DocumentKey, Mutation>();
          keys.forEach(key => {
            if (!processed.has(key)) {
              overlays.set(
                key,
                calculateOverlayMutation(docs.get(key)!, masks.get(key)!)
              );
              processed.add(key);
            }
          });
          promises.push(
            this.documentOverlayCache.saveOverlays(
              transaction,
              batchId,
              overlays
            )
          );
        }
        return PersistencePromise.waitFor(promises);
      });
  }

  /**
   * Recalculates overlays by reading the documents from remote document cache
   * first, and saves them after they are calculated.
   */
  recalculateAndSaveOverlaysForDocumentKeys(
    transaction: PersistenceTransaction,
    documentKeys: DocumentKeySet
  ): PersistencePromise<void> {
    return this.remoteDocumentCache
      .getEntries(transaction, documentKeys)
      .next(docs => {
        return this.recalculateAndSaveOverlays(transaction, docs);
      });
  }

  /**
   * Performs a query against the local view of all documents.
   *
   * @param transaction - The persistence transaction.
   * @param query - The query to match documents against.
   * @param sinceReadTime - If not set to SnapshotVersion.min(), return only
   *     documents that have been read since this snapshot version (exclusive).
   */
  getDocumentsMatchingQuery(
    transaction: PersistenceTransaction,
    query: Query,
    offset: IndexOffset
  ): PersistencePromise<DocumentMap> {
    if (isDocumentQuery(query)) {
      return this.getDocumentsMatchingDocumentQuery(transaction, query.path);
    } else if (isCollectionGroupQuery(query)) {
      return this.getDocumentsMatchingCollectionGroupQuery(
        transaction,
        query,
        offset
      );
    } else {
      return this.getDocumentsMatchingCollectionQuery(
        transaction,
        query,
        offset
      );
    }
  }

  private getDocumentsMatchingDocumentQuery(
    transaction: PersistenceTransaction,
    docPath: ResourcePath
  ): PersistencePromise<DocumentMap> {
    // Just do a simple document lookup.
    return this.getDocument(transaction, new DocumentKey(docPath)).next(
      document => {
        let result = documentMap();
        if (document.isFoundDocument()) {
          result = result.insert(document.key, document);
        }
        return result;
      }
    );
  }

  private getDocumentsMatchingCollectionGroupQuery(
    transaction: PersistenceTransaction,
    query: Query,
    offset: IndexOffset
  ): PersistencePromise<DocumentMap> {
    debugAssert(
      query.path.isEmpty(),
      'Currently we only support collection group queries at the root.'
    );
    const collectionId = query.collectionGroup!;
    let results = documentMap();
    return this.indexManager
      .getCollectionParents(transaction, collectionId)
      .next(parents => {
        // Perform a collection query against each parent that contains the
        // collectionId and aggregate the results.
        return PersistencePromise.forEach(parents, (parent: ResourcePath) => {
          const collectionQuery = asCollectionQueryAtPath(
            query,
            parent.child(collectionId)
          );
          return this.getDocumentsMatchingCollectionQuery(
            transaction,
            collectionQuery,
            offset
          ).next(r => {
            r.forEach((key, doc) => {
              results = results.insert(key, doc);
            });
          });
        }).next(() => results);
      });
  }

  private getDocumentsMatchingCollectionQuery(
    transaction: PersistenceTransaction,
    query: Query,
    offset: IndexOffset
  ): PersistencePromise<DocumentMap> {
    let remoteDocuments = mutableDocumentMap();
    return this.remoteDocumentCache
      .getDocumentsMatchingQuery(transaction, query, offset.readTime)
      .next(queryResults => {
        remoteDocuments = queryResults;
        return this.documentOverlayCache.getOverlaysForCollection(
          transaction,
          query.path,
          offset.largestBatchId
        );
      })
      .next(overlays => {
        // As documents might match the query because of their overlay we need to
        // include documents for all overlays in the initial document set.
        overlays.forEach(overlay => {
          const key = overlay.getKey();
          if (remoteDocuments.get(key) === null) {
            remoteDocuments.insert(
              key,
              MutableDocument.newInvalidDocument(key)
            );
          }
        });

        // Apply the overlays and match against the query.
        let results = documentMap();
        remoteDocuments.forEach((key, document) => {
          const overlay = overlays.get(key);
          if (overlay !== undefined) {
            mutationApplyToLocalView(
              overlay.mutation,
              document,
              null,
              Timestamp.now()
            );
          }
          // Finally, insert the documents that still match the query
          if (queryMatches(query, document)) {
            results = results.insert(key, document);
          }
        });
        return results;
      });
  }

  // private getDocumentsMatchingCollectionQuery(
  //   transaction: PersistenceTransaction,
  //   query: Query,
  //   sinceReadTime: SnapshotVersion
  // ): PersistencePromise<DocumentMap> {
  //   // Query the remote documents and overlay mutations.
  //   let results: MutableDocumentMap;
  //   let mutationBatches: MutationBatch[];
  //   return this.remoteDocumentCache
  //     .getDocumentsMatchingQuery(transaction, query, sinceReadTime)
  //     .next(queryResults => {
  //       results = queryResults;
  //       return this.mutationQueue.getAllMutationBatchesAffectingQuery(
  //         transaction,
  //         query
  //       );
  //     })
  //     .next(matchingMutationBatches => {
  //       mutationBatches = matchingMutationBatches;
  //       // It is possible that a PatchMutation can make a document match a query, even if
  //       // the version in the RemoteDocumentCache is not a match yet (waiting for server
  //       // to ack). To handle this, we find all document keys affected by the PatchMutations
  //       // that are not in `result` yet, and back fill them via `remoteDocumentCache.getEntries`,
  //       // otherwise those `PatchMutations` will be ignored because no base document can be found,
  //       // and lead to missing result for the query.
  //       return this.addMissingBaseDocuments(
  //         transaction,
  //         mutationBatches,
  //         results
  //       ).next(mergedDocuments => {
  //         results = mergedDocuments;
  //
  //         for (const batch of mutationBatches) {
  //           for (const mutation of batch.mutations) {
  //             const key = mutation.key;
  //             let document = results.get(key);
  //             if (document == null) {
  //               // Create invalid document to apply mutations on top of
  //               document = MutableDocument.newInvalidDocument(key);
  //               results = results.insert(key, document);
  //             }
  //             mutationApplyToLocalView(
  //               mutation,
  //               document,
  //               null,
  //               batch.localWriteTime
  //             );
  //             if (!document.isFoundDocument()) {
  //               results = results.remove(key);
  //             }
  //           }
  //         }
  //       });
  //     })
  //     .next(() => {
  //       // Finally, filter out any documents that don't actually match
  //       // the query.
  //       results.forEach((key, doc) => {
  //         if (!queryMatches(query, doc)) {
  //           results = results.remove(key);
  //         }
  //       });
  //
  //       return results as DocumentMap;
  //     });
  // }

  // private addMissingBaseDocuments(
  //   transaction: PersistenceTransaction,
  //   matchingMutationBatches: MutationBatch[],
  //   existingDocuments: MutableDocumentMap
  // ): PersistencePromise<MutableDocumentMap> {
  //   let missingBaseDocEntriesForPatching = documentKeySet();
  //   for (const batch of matchingMutationBatches) {
  //     for (const mutation of batch.mutations) {
  //       if (
  //         mutation instanceof PatchMutation &&
  //         existingDocuments.get(mutation.key) === null
  //       ) {
  //         missingBaseDocEntriesForPatching =
  //           missingBaseDocEntriesForPatching.add(mutation.key);
  //       }
  //     }
  //   }
  //
  //   let mergedDocuments = existingDocuments;
  //   return this.remoteDocumentCache
  //     .getEntries(transaction, missingBaseDocEntriesForPatching)
  //     .next(missingBaseDocs => {
  //       missingBaseDocs.forEach((key, doc) => {
  //         if (doc.isFoundDocument()) {
  //           mergedDocuments = mergedDocuments.insert(key, doc);
  //         }
  //       });
  //       return mergedDocuments;
  //     });
  // }

  /** Returns a base document that can be used to apply `overlay`. */
  private getBaseDocument(
    transaction: PersistenceTransaction,
    key: DocumentKey,
    overlay: Overlay | null
  ): PersistencePromise<MutableDocument> {
    return overlay == null || overlay.mutation instanceof PatchMutation
      ? this.remoteDocumentCache.getEntry(transaction, key)
      : PersistencePromise.resolve(MutableDocument.newInvalidDocument(key));
  }
}
