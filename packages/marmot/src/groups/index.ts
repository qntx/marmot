/**
 * `groups` — thin typed functions over `Engine` for the client pump: group lifecycle and messaging
 * wrappers, plus the three-step media send (seal in the pump, upload outside it, send back in the
 * pump) so uploads never hold the serialized engine queue.
 */

import type { Engine } from "../backend/engine.ts";
import type {
  AppMessage,
  AttachmentInput,
  ComponentUpdate,
  GroupSnapshot,
  Invitee,
  Locator,
  MediaAttachmentRef,
  SendOutcome,
} from "../backend/protocol.ts";

export async function createGroup(
  engine: Engine,
  args: Parameters<Engine["createGroup"]>[0],
): Promise<GroupSnapshot> {
  return engine.createGroup(args);
}

export async function invite(
  engine: Engine,
  group: string,
  invitees: Invitee[],
): Promise<SendOutcome> {
  return engine.invite(group, invitees);
}

export async function removeMembers(
  engine: Engine,
  group: string,
  members: string[],
): Promise<SendOutcome> {
  return engine.removeMembers(group, members);
}

export async function leave(engine: Engine, group: string): Promise<SendOutcome> {
  return engine.leave(group);
}

export async function selfUpdate(engine: Engine, group: string): Promise<SendOutcome> {
  return engine.selfUpdate(group);
}

export async function updateGroupProfile(
  engine: Engine,
  group: string,
  profile: { name?: string | undefined; description?: string | undefined },
): Promise<SendOutcome> {
  return engine.updateGroupProfile(group, profile.name, profile.description);
}

export async function updateAppComponents(
  engine: Engine,
  group: string,
  updates: ComponentUpdate[],
): Promise<SendOutcome> {
  return engine.updateAppComponents(group, updates);
}

export async function sendMessage(
  engine: Engine,
  group: string,
  message: AppMessage,
): Promise<SendOutcome> {
  return engine.sendMessage(group, message);
}

/** One blob upload: ciphertext plus the metadata the store needs; resolves with its locators. */
export type UploadPort = (blob: {
  ciphertext: Uint8Array;
  sha256: string;
  mediaType: string;
  filename: string;
}) => Promise<Locator[]>;

/** An upload failed; sealed handles are the caller's to discard. Fixed message, no payload. */
export class UploadFailedError extends Error {
  constructor() {
    super("marmot: upload failed");
    this.name = "UploadFailedError";
  }
}

/** A sealed attachment handle plus the ciphertext to upload. */
export type Sealed = {
  handle: number;
  ciphertext: Uint8Array;
  ciphertextSha256: string;
};

/**
 * Seal every attachment at the group's current media epoch. A failure discards the handles already
 * sealed before rethrowing.
 */
export async function sealAttachments(
  engine: Engine,
  group: string,
  attachments: AttachmentInput[],
): Promise<Sealed[]> {
  const sealed: Sealed[] = [];
  try {
    for (const attachment of attachments) {
      // oxlint-disable-next-line no-await-in-loop -- sealing is sequential by design
      sealed.push(await engine.sealAttachment(group, attachment));
    }
    return sealed;
  } catch (error) {
    try {
      await discardSealed(engine, sealed);
    } catch {
      // The seal failure is what propagates.
    }
    throw error;
  }
}

/**
 * Upload every sealed blob through `upload`, paired by index with `attachments` for the metadata.
 * Any upload throw becomes `UploadFailedError`.
 */
export async function uploadSealed(
  upload: UploadPort,
  sealed: Sealed[],
  attachments: AttachmentInput[],
): Promise<MediaAttachmentRef[]> {
  const refs: MediaAttachmentRef[] = [];
  try {
    for (const [i, item] of sealed.entries()) {
      const attachment = attachments[i];
      if (attachment === undefined) {
        break;
      }
      // oxlint-disable-next-line no-await-in-loop -- uploads are sequential by design
      const locators = await upload({
        ciphertext: item.ciphertext,
        sha256: item.ciphertextSha256,
        mediaType: attachment.mediaType,
        filename: attachment.filename,
      });
      refs.push({ handle: item.handle, locators });
    }
    return refs;
  } catch {
    throw new UploadFailedError();
  }
}

/** Send a media message carrying already-uploaded sealed attachments. */
export async function sendSealedMedia(
  engine: Engine,
  group: string,
  caption: string,
  refs: MediaAttachmentRef[],
  tags: string[][] = [],
): Promise<SendOutcome> {
  return engine.sendMedia(group, caption, refs, tags);
}

/** Drop every sealed handle (on upload failure or abandon); rethrows the first failure. */
export async function discardSealed(engine: Engine, sealed: Sealed[]): Promise<void> {
  let first: Error | undefined;
  for (const item of sealed) {
    try {
      // oxlint-disable-next-line no-await-in-loop -- discards are sequential by design
      await engine.discardAttachment(item.handle);
    } catch (error) {
      first ??= error instanceof Error ? error : new Error(String(error));
    }
  }
  if (first !== undefined) {
    throw first;
  }
}
