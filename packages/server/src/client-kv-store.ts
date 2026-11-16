export * as ClientKVStore from "./client-kv-store"

import { Bus } from "@opencode/core/bus"
import { KV } from "@opencode/core/kv"
import { Entry, Event, Key, Revision, Space, Value } from "@opencode/schema/client-kv"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { Context, Effect, Layer, Option, Schema, Semaphore } from "effect"

export interface Request {
  readonly space: Space
  /** The last revision the client applied; a delta is sent only when this server issued it. */
  readonly since?: number
}

export type Initial =
  | { readonly type: typeof Event.Snapshot.type; readonly data: typeof Event.Snapshot.data.Type }
  | { readonly type: typeof Event.Delta.type; readonly data: typeof Event.Delta.data.Type }

export interface Interface {
  readonly set: (space: Space, key: Key, value: Value) => Effect.Effect<void>
  readonly remove: (space: Space, key: Key) => Effect.Effect<void>
  /**
   * Runs `register`, which must start buffering this store's events, then reads each space, with no write in
   * between: every write is then either in the initial state or delivered to the registration, never both or neither.
   */
  readonly open: <A, E, R>(
    requests: ReadonlyArray<Request>,
    register: Effect.Effect<A, E, R>,
  ) => Effect.Effect<readonly [A, ReadonlyArray<Initial>], E, R>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/server/ClientKVStore") {}

// Each key is one row holding its value or, once deleted, a tombstone, with the revision of its last write. A write
// is one row upsert, so a crash never leaves a value and its revision apart, and a space's revision is the highest
// one among its rows, so it is never reissued after a restart.
const Stored = Schema.Union([
  Schema.Struct({ revision: Revision, value: Value }),
  Schema.Struct({ revision: Revision, deleted: Schema.Literal(true) }),
])
const decodeStored = Schema.decodeUnknownOption(Stored)

interface Row {
  readonly key: Key
  readonly stored: typeof Stored.Type
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const kv = yield* KV.Service
    const bus = yield* Bus.Service
    // Bus listeners run inline, so publishing under the lock queues the event for every registered subscriber before
    // the next write or read starts, and subscribers see writes in revision order.
    const lock = yield* Semaphore.make(1)
    const revisions = new Map<Space, Revision>()
    // Spaces cannot contain ":", so keys in different spaces never collide.
    const prefix = (space: Space) => `client:${space}:`

    const read = Effect.fnUntraced(function* (space: Space) {
      const rows: Row[] = []
      let after: string | undefined
      while (true) {
        const page = yield* kv.scan({ prefix: prefix(space), after, limit: 1000 })
        rows.push(
          ...page.entries.flatMap((entry) =>
            Option.match(decodeStored(entry.value), {
              onNone: () => [],
              onSome: (stored) => [{ key: entry.key.slice(prefix(space).length), stored }],
            }),
          ),
        )
        if (page.next !== undefined) {
          after = page.next
          continue
        }
        const revision = rows.reduce((max, row) => Math.max(max, row.stored.revision), 0)
        revisions.set(space, revision)
        return { rows, revision }
      }
    })
    const next = Effect.fnUntraced(function* (space: Space) {
      return (revisions.get(space) ?? (yield* read(space)).revision) + 1
    })
    const write = (space: Space, key: Key, stored: typeof Stored.Type) =>
      kv
        .set(prefix(space) + key, stored)
        .pipe(Effect.tap(() => Effect.sync(() => revisions.set(space, stored.revision))))

    return Service.of({
      // Uninterruptible so a dropped request cannot commit a write that subscribers never hear about.
      set: Effect.fn("ClientKVStore.set")(function* (space, key, value) {
        yield* lock.withPermit(
          Effect.uninterruptible(
            Effect.gen(function* () {
              const revision = yield* next(space)
              yield* write(space, key, { revision, value })
              yield* bus.publish(Event.Updated, { space, key, value, revision })
            }),
          ),
        )
      }),
      remove: Effect.fn("ClientKVStore.remove")(function* (space, key) {
        yield* lock.withPermit(
          Effect.uninterruptible(
            Effect.gen(function* () {
              const stored = decodeStored(yield* kv.get(prefix(space) + key))
              if (Option.isNone(stored) || "deleted" in stored.value) return
              const revision = yield* next(space)
              yield* write(space, key, { revision, deleted: true })
              yield* bus.publish(Event.Deleted, { space, key, revision })
            }),
          ),
        )
      }),
      open: (requests, register) =>
        lock.withPermit(
          Effect.gen(function* () {
            const registered = yield* register
            const initial = yield* Effect.forEach(requests, (request) =>
              read(request.space).pipe(Effect.map((state) => initialState(request, state.rows, state.revision))),
            )
            return [registered, initial] as const
          }),
        ),
    })
  }),
)

function initialState(request: Request, rows: ReadonlyArray<Row>, revision: Revision): Initial {
  const since = request.since
  const entries = (changed: ReadonlyArray<Row>): Entry[] =>
    changed.flatMap((row) => ("value" in row.stored ? [{ key: row.key, value: row.stored.value }] : []))
  if (since === undefined || since > revision)
    return { type: Event.Snapshot.type, data: { space: request.space, revision, entries: entries(rows) } }
  const changed = rows.filter((row) => row.stored.revision > since)
  return {
    type: Event.Delta.type,
    data: {
      space: request.space,
      revision,
      entries: entries(changed),
      deleted: changed.flatMap((row) => ("deleted" in row.stored ? [row.key] : [])),
    },
  }
}

export const node = makeGlobalNode({ service: Service, layer, deps: [KV.node, Bus.node] })
