import { expect } from "bun:test"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { KV } from "@opencode/core/kv"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Context, Deferred, Effect, Fiber, Layer, Stream } from "effect"
import { it } from "../../core/test/lib/effect"
import { ClientKVStore } from "../src/client-kv-store"
import { EventFeed } from "../src/event-feed"

// One in-memory database shared by every `boot()`, each a fresh store, bus and feed as after a process restart.
// `hold(key)` pauses the next write of that stored key after it commits, while the store lock is still held, so a test
// can line up competing writes and subscriptions behind it.
const setup = Effect.gen(function* () {
  const kv = Context.get(yield* Layer.build(LayerNode.compile(KV.node)), KV.Service)
  const gates = new Map<string, { committed: Deferred.Deferred<void>; release: Deferred.Deferred<void> }>()
  const paused = KV.Service.of({
    ...kv,
    set: (key, value) =>
      kv.set(key, value).pipe(
        Effect.andThen(
          Effect.suspend(() => {
            const gate = gates.get(key)
            if (!gate) return Effect.void
            gates.delete(key)
            return Deferred.succeed(gate.committed, undefined).pipe(Effect.andThen(Deferred.await(gate.release)))
          }),
        ),
      ),
  })
  const boot = Effect.fnUntraced(function* () {
    const context = yield* Layer.build(
      LayerNode.compile(LayerNode.group([ClientKVStore.node, Bus.node]), {
        replacements: [
          KV.node.replace(makeGlobalNode({ service: KV.Service, layer: Layer.succeed(KV.Service, paused), deps: [] })),
        ],
      }),
    )
    const bus = Context.get(context, Bus.Service)
    return { store: Context.get(context, ClientKVStore.Service), bus, feed: yield* EventFeed.make(bus.listen) }
  })
  const hold = Effect.fnUntraced(function* (space: string, key: string) {
    const gate = { committed: yield* Deferred.make<void>(), release: yield* Deferred.make<void>() }
    gates.set(`client:${space}:${key}`, gate)
    return gate
  })
  return { boot, hold }
})

const take = (live: Stream.Stream<string, EventFeed.Error>, count: number) =>
  live.pipe(
    Stream.take(count),
    Stream.runCollect,
    Effect.map((frames) =>
      Array.from(frames, (frame) => {
        const event = JSON.parse(frame.slice("data: ".length))
        return { type: event.type, data: event.data }
      }),
    ),
  )

const now = { startImmediately: true } as const

it.live("delivers racing writes in revision order", () =>
  Effect.gen(function* () {
    const { boot, hold } = yield* setup
    const { store, feed } = yield* boot()
    const [live] = yield* store.open([{ space: "app" }], feed.subscribe(undefined, ["app"]))

    const first = yield* hold("app", "k")
    const a = yield* store.set("app", "k", "a").pipe(Effect.forkChild(now))
    yield* Deferred.await(first.committed)
    // Blocked behind "a": without the lock it would commit and publish "b" before "a" is published.
    const b = yield* store.set("app", "k", "b").pipe(Effect.forkChild(now))
    yield* Deferred.succeed(first.release, undefined)
    yield* Fiber.join(a)
    yield* Fiber.join(b)

    expect(yield* take(live, 2)).toEqual([
      { type: "experimental.kv.updated", data: { space: "app", key: "k", value: "a", revision: 1 } },
      { type: "experimental.kv.updated", data: { space: "app", key: "k", value: "b", revision: 2 } },
    ])
  }),
)

it.live("a subscription opened during a write gets it in its initial state or as an update, exactly once", () =>
  Effect.gen(function* () {
    const { boot, hold } = yield* setup
    const { store, feed } = yield* boot()
    yield* store.set("app", "k", "old")
    yield* store.set("app", "gone", 1)

    const pending = yield* hold("app", "k")
    const write = yield* store.set("app", "k", "new").pipe(Effect.forkChild(now))
    yield* Deferred.await(pending.committed)
    // "new" (revision 3) is committed but not yet published. Everything below queues behind it, in this order.
    const snapshot = yield* store
      .open([{ space: "app" }], feed.subscribe(undefined, ["app"]))
      .pipe(Effect.forkChild(now))
    const delta = yield* store
      .open([{ space: "app", since: 2 }], feed.subscribe(undefined, ["app"]))
      .pipe(Effect.forkChild(now))
    const next = yield* store.set("app", "k", "newer").pipe(Effect.forkChild(now))
    yield* Deferred.succeed(pending.release, undefined)
    yield* Fiber.join(write)
    const [fresh, snapshots] = yield* Fiber.join(snapshot)
    const [resumed, deltas] = yield* Fiber.join(delta)
    yield* Fiber.join(next)
    yield* store.remove("app", "k")

    expect(snapshots).toEqual([
      {
        type: "experimental.kv.snapshot",
        data: {
          space: "app",
          revision: 3,
          entries: [
            { key: "gone", value: 1 },
            { key: "k", value: "new" },
          ],
        },
      },
    ])
    expect(deltas).toEqual([
      {
        type: "experimental.kv.delta",
        data: { space: "app", revision: 3, entries: [{ key: "k", value: "new" }], deleted: [] },
      },
    ])
    // Without the lock both would register before "new" is published and receive it again after their initial state.
    const after = [
      { type: "experimental.kv.updated", data: { space: "app", key: "k", value: "newer", revision: 4 } },
      { type: "experimental.kv.deleted", data: { space: "app", key: "k", revision: 5 } },
    ]
    expect(yield* take(fresh, 2)).toEqual(after)
    expect(yield* take(resumed, 2)).toEqual(after)
  }),
)

it.live("catches each space up from its revision: final values and deletions, else a snapshot", () =>
  Effect.gen(function* () {
    const { boot } = yield* setup
    const { store, feed } = yield* boot()
    yield* store.set("app", "kept", "v1")
    yield* store.set("app", "churn", 1)
    yield* store.set("app", "doomed", true)
    yield* store.set("other", "x", "y")
    const cursor = 3

    // While the client is away: repeated updates, a delete, a delete repeated, and a delete then re-set.
    yield* store.set("app", "churn", 2)
    yield* store.set("app", "churn", 3)
    yield* store.remove("app", "doomed")
    yield* store.remove("app", "doomed")
    yield* store.remove("app", "never-set")
    yield* store.set("app", "back", "a")
    yield* store.remove("app", "back")
    yield* store.set("app", "back", "b")

    const [, initial] = yield* store.open(
      [
        { space: "app", since: cursor },
        { space: "other", since: 1 },
        { space: "fresh" },
        { space: "ahead", since: 9 },
        { space: "zero", since: 0 },
      ],
      Effect.void,
    )
    expect(initial).toEqual([
      {
        type: "experimental.kv.delta",
        data: {
          space: "app",
          revision: 9,
          entries: [
            { key: "back", value: "b" },
            { key: "churn", value: 3 },
          ],
          deleted: ["doomed"],
        },
      },
      { type: "experimental.kv.delta", data: { space: "other", revision: 1, entries: [], deleted: [] } },
      { type: "experimental.kv.snapshot", data: { space: "fresh", revision: 0, entries: [] } },
      { type: "experimental.kv.snapshot", data: { space: "ahead", revision: 0, entries: [] } },
      { type: "experimental.kv.delta", data: { space: "zero", revision: 0, entries: [], deleted: [] } },
    ])

    const [, current] = yield* store.open([{ space: "app", since: 9 }], feed.subscribe(undefined, ["app"]))
    expect(current).toEqual([
      { type: "experimental.kv.delta", data: { space: "app", revision: 9, entries: [], deleted: [] } },
    ])
  }),
)

it.live("a restarted store resumes revisions and catch-up from storage alone", () =>
  Effect.gen(function* () {
    const { boot } = yield* setup
    const before = yield* boot()
    yield* before.store.set("app", "k", "a")
    yield* before.store.set("app", "k", "b")

    // A new process over the same database. Its first write, before any read, must not reuse a revision.
    const after = yield* boot()
    yield* after.store.set("app", "j", "c")
    const [live, initial] = yield* after.store.open(
      [{ space: "app", since: 1 }],
      after.feed.subscribe(undefined, ["app"]),
    )
    const [, latest] = yield* after.store.open([{ space: "app", since: 2 }], Effect.void)
    expect([...initial, ...latest]).toEqual([
      {
        type: "experimental.kv.delta",
        data: {
          space: "app",
          revision: 3,
          entries: [
            { key: "j", value: "c" },
            { key: "k", value: "b" },
          ],
          deleted: [],
        },
      },
      {
        type: "experimental.kv.delta",
        data: { space: "app", revision: 3, entries: [{ key: "j", value: "c" }], deleted: [] },
      },
    ])
    yield* after.store.remove("app", "k")
    expect(yield* take(live, 1)).toEqual([
      { type: "experimental.kv.deleted", data: { space: "app", key: "k", revision: 4 } },
    ])
  }),
)

it.live("sends kv events only to subscribers of their space and leaves other events alone", () =>
  Effect.gen(function* () {
    const { boot } = yield* setup
    const { store, feed, bus } = yield* boot()
    const app = yield* feed.subscribe([Agent.Event.Updated.type], ["app"])
    const other = yield* feed.subscribe(undefined, ["other"])
    const plain = yield* feed.subscribe()

    yield* store.set("app", "k", { n: 1 })
    yield* bus.publish(Agent.Event.Updated, {})

    expect(yield* take(app, 2)).toEqual([
      { type: "experimental.kv.updated", data: { space: "app", key: "k", value: { n: 1 }, revision: 1 } },
      { type: "agent.updated", data: {} },
    ])
    expect(yield* take(other, 1)).toEqual([{ type: "agent.updated", data: {} }])
    expect(yield* take(plain, 1)).toEqual([{ type: "agent.updated", data: {} }])
  }),
)
