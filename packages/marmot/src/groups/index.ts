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
  GroupAvatar,
  GroupSnapshot,
  Invitee,
  Locator,
  MediaAttachmentRef,
  ReaddOutcome,
  RejoinOffer,
  SendOutcome,
  SentMessage,
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
): Promise<SentMessage> {
  return engine.sendMessage(group, message);
}

/** Send a chat message; `replyTo` adds the `e`/`q` reply pair. */
export async function sendChat(
  engine: Engine,
  group: string,
  content: string,
  replyTo?: string,
): Promise<SentMessage> {
  return engine.sendIntent(group, { type: "chat", content, replyTo });
}

/** React to `target` with `emoji`. */
export async function react(
  engine: Engine,
  group: string,
  target: string,
  emoji: string,
): Promise<SentMessage> {
  return engine.sendIntent(group, { type: "reaction", target, emoji });
}

/** Withdraw own reactions by event id. */
export async function unreact(
  engine: Engine,
  group: string,
  reactions: string[],
): Promise<SentMessage> {
  return engine.sendIntent(group, { type: "unreact", reactions });
}

/** Replace `target`'s content. */
export async function editMessage(
  engine: Engine,
  group: string,
  target: string,
  content: string,
): Promise<SentMessage> {
  return engine.sendIntent(group, { type: "edit", target, content });
}

/** Delete `target` (admin removal shape when the sender is an admin). */
export async function deleteMessage(
  engine: Engine,
  group: string,
  target: string,
): Promise<SentMessage> {
  return engine.sendIntent(group, { type: "delete", target });
}

/** Admin removal of `target`. */
export async function removeMessage(
  engine: Engine,
  group: string,
  target: string,
): Promise<SentMessage> {
  return engine.sendIntent(group, { type: "remove", target });
}

/** Grant or revoke `account`'s admin flag. */
export async function setAdmin(
  engine: Engine,
  group: string,
  account: string,
  admin: boolean,
): Promise<SendOutcome> {
  return engine.setAdmin(group, account, admin);
}

/** Step down: revoke this account's own admin flag. */
export async function stepDown(
  engine: Engine,
  group: string,
  account: string,
): Promise<SendOutcome> {
  return engine.setAdmin(group, account, false);
}

/** Set or clear the group's avatar; `undefined` clears. */
export async function setGroupAvatar(
  engine: Engine,
  group: string,
  avatar: GroupAvatar | undefined,
): Promise<SendOutcome> {
  return engine.setGroupAvatar(group, avatar);
}

/** Set the group's message retention seconds; `0` disables. */
export async function setRetention(
  engine: Engine,
  group: string,
  seconds: bigint,
): Promise<SendOutcome> {
  return engine.setRetention(group, seconds);
}

/** Request the group's terminal disband. */
export async function disband(engine: Engine, group: string): Promise<SendOutcome> {
  return engine.disband(group);
}

/** Require the lifecycle component on a group that predates it. */
export async function enableDisbanding(engine: Engine, group: string): Promise<SendOutcome> {
  return engine.enableDisbanding(group);
}

/** Forget the group locally. */
export async function forgetGroup(engine: Engine, group: string): Promise<void> {
  return engine.forgetGroup(group);
}

/** The group's stored replacement-Welcome offers awaiting consent. */
export async function rejoinOffers(engine: Engine, group: string): Promise<RejoinOffer[]> {
  return engine.rejoinOffers(group);
}

/** Consent to a stored replacement Welcome — installs the new copy. */
export async function confirmRejoin(
  engine: Engine,
  group: string,
  offer: string,
  token: string,
): Promise<void> {
  return engine.confirmRejoin(group, offer, token);
}

/** Decline a stored replacement Welcome — retires the offer. */
export async function declineRejoin(engine: Engine, offer: string): Promise<void> {
  return engine.declineRejoin(offer);
}

/** Remove a stuck member account, then re-invite it once the removal confirms. */
export async function readdMember(
  engine: Engine,
  group: string,
  account: string,
  invitees: Invitee[],
): Promise<ReaddOutcome> {
  return engine.readdMember(group, account, invitees);
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
): Promise<SentMessage> {
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
