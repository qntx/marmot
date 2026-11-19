/**
 * `indexedDbCursorStore` — the client's sync-cursor blob over IndexedDB: one sealed row under the
 * fixed key `marmot-cursors` in its own database and `cursor` object store. The row key is also the
 * cipher's associated-data key, so a sealed value cannot be replayed under another name.
 */

import { utf8Encode } from "../backend/utf8.ts";
import { StorageError } from "./driver.ts";
import type { Cipher, CursorStore } from "./driver.ts";
import { requestOnce, toBytes, txnDone } from "./indexeddb.ts";
import type { IdbDatabaseLike, IdbFactoryLike } from "./indexeddb.ts";

const STORE = "cursor";
const ROW_KEY = utf8Encode("marmot-cursors");

async function openCursorDatabase(factory: IdbFactoryLike, name: string): Promise<IdbDatabaseLike> {
  return new Promise<IdbDatabaseLike>((resolve, reject) => {
    const request = factory.open(name, 1);
    request.addEventListener("upgradeneeded", () => {
      request.result.createObjectStore(STORE);
    });
    request.addEventListener("success", () => {
      resolve(request.result);
    });
    request.addEventListener("error", () => {
      reject(new StorageError("io"));
    });
  });
}

/** Open (creating if needed) the `cursor` store in database `name`, sealed with `cipher`. */
export async function indexedDbCursorStore(
  factory: IdbFactoryLike,
  name: string,
  options: { cipher: Cipher },
): Promise<CursorStore & { close: () => Promise<void> }> {
  const db = await openCursorDatabase(factory, name);
  let closed = false;
  const requireOpen = (): void => {
    if (closed) {
      throw new StorageError("closed");
    }
  };
  return {
    load: async () => {
      requireOpen();
      const txn = db.transaction(STORE, "readonly");
      const row = await requestOnce(txn.objectStore(STORE).get(ROW_KEY));
      const sealed = toBytes(row);
      if (sealed === undefined) {
        return undefined;
      }
      try {
        return await options.cipher.open(ROW_KEY, sealed);
      } catch {
        return undefined;
      }
    },
    save: async (bytes) => {
      requireOpen();
      const sealed = await options.cipher.seal(ROW_KEY, bytes);
      const txn = db.transaction(STORE, "readwrite");
      txn.objectStore(STORE).put(sealed, ROW_KEY);
      await txnDone(txn);
    },
    close: async () => {
      closed = true;
      db.close();
      await Promise.resolve();
    },
  };
}
