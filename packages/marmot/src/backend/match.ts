import { DecodeError } from "./errors.ts";

/**
 * Statically exhaustive dispatch on a `type`-tagged union: `cases` must provide an arm for every
 * variant, and each arm receives the value narrowed to that variant.
 */
export function match<T extends { readonly type: string }, R>(
  value: T,
  cases: { [K in T["type"]]: (variant: Extract<T, { type: K }>) => R },
): R {
  // oxlint-disable-next-line no-unsafe-type-assertion -- the table maps each tag to an arm narrowed to that variant
  const arms = cases as unknown as Record<string, (variant: T) => R>;
  const arm = arms[value.type];
  if (arm === undefined) {
    throw new DecodeError("match", "unreachable");
  }
  return arm(value);
}
