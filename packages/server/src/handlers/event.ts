import { Event } from "@opencode/schema/event"
import { Clock, Effect, Stream } from "effect"
import { HttpServerResponse } from "effect/http"
import { HttpApiBuilder } from "effect/http-api"
import { Api } from "../api"
import { ClientKVStore } from "../client-kv-store"
import { EventFeed } from "../event-feed"

export const EventHandler = HttpApiBuilder.group(Api, "server.event", (handlers) =>
  Effect.gen(function* () {
    const feed = yield* EventFeed.Service
    const store = yield* ClientKVStore.Service
    return handlers.handleRaw("event.subscribe", (ctx) =>
      Effect.gen(function* () {
        const connected = {
          id: Event.ID.create(),
          type: "server.connected",
          data: {},
        } as const
        const requests = Array.from(
          new Map((ctx.query["experimental.kv"] ?? []).map(parse).map((request) => [request.space, request])).values(),
        )
        const output = Stream.unwrap(
          Effect.gen(function* () {
            if (requests.length === 0) {
              const live = yield* feed.subscribe(ctx.query.types)
              return Stream.make(EventFeed.frame(connected)).pipe(Stream.concat(live))
            }
            const [live, initial] = yield* store.open(
              requests,
              feed.subscribe(
                ctx.query.types,
                requests.map((request) => request.space),
              ),
            )
            const created = yield* Clock.currentTimeMillis
            return Stream.make(
              EventFeed.frame(connected),
              ...initial.map((item) => EventFeed.frame({ id: Event.ID.create(), created, ...item })),
            ).pipe(Stream.concat(live))
          }),
        )
        const heartbeat = Stream.tick("15 seconds").pipe(Stream.map(() => ": heartbeat\n\n"))
        return HttpServerResponse.stream(
          output.pipe(Stream.merge(heartbeat, { haltStrategy: "left" }), Stream.encodeText),
          {
            contentType: "text/event-stream",
            headers: {
              "Cache-Control": "no-cache, no-transform",
              "X-Accel-Buffering": "no",
              "X-Content-Type-Options": "nosniff",
            },
          },
        )
      }),
    )
  }),
)

// `space:revision`; a revision that is not a plain non-negative integer is unusable, so the space gets a snapshot.
function parse(item: string): ClientKVStore.Request {
  const index = item.indexOf(":")
  if (index < 0) return { space: item }
  const space = item.slice(0, index)
  const revision = item.slice(index + 1)
  if (!/^\d+$/u.test(revision) || !Number.isSafeInteger(Number(revision))) return { space }
  return { space, since: Number(revision) }
}
