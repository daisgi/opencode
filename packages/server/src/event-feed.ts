export * as EventFeed from "./event-feed"

import { Bus } from "@opencode/core/bus"
import { ClientKV } from "@opencode/schema/client-kv"
import { Event } from "@opencode/schema/event"
import { isOpenCodeEvent, type OpenCodeEvent } from "@opencode/protocol/groups/event"
import { Cause, Context, Effect, Layer, Queue, Schema, Scope, Stream } from "effect"

export const SubscriberCapacity = 4_096

export class SubscriberOverflowError extends Schema.TaggedError<SubscriberOverflowError>()(
  "EventFeed.SubscriberOverflow",
  { capacity: Schema.Int },
) {}

export class EncodingError extends Schema.TaggedError<EncodingError>()("EventFeed.EncodingError", {
  eventID: Event.ID,
  eventType: Schema.String,
  cause: Schema.Defect(),
}) {}

export type Error = SubscriberOverflowError | EncodingError

export interface Interface {
  /** `experimental.kv.*` events reach only subscribers that list their space in `spaces`, whatever `types` says. */
  readonly subscribe: (
    types?: ReadonlyArray<string>,
    spaces?: ReadonlyArray<string>,
  ) => Effect.Effect<Stream.Stream<string, Error>, never, Scope.Scope>
}

interface Filter {
  readonly types?: ReadonlySet<string>
  readonly spaces: ReadonlySet<string>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/server/EventFeed") {}

const isKVEvent = Schema.is(Schema.Union([ClientKV.Event.Updated, ClientKV.Event.Deleted]))

export function frame(event: OpenCodeEvent) {
  return `data: ${JSON.stringify(event)}\n\n`
}

export const make = Effect.fn("EventFeed.make")(function* (
  observe: (subscriber: Bus.Subscriber) => Effect.Effect<Bus.Unsubscribe>,
  options?: { readonly capacity?: number; readonly encode?: (event: OpenCodeEvent) => string },
) {
  const capacity = options?.capacity ?? SubscriberCapacity
  const render = options?.encode ?? frame
  const subscribers = new Map<Queue.Queue<string, Error>, Filter>()

  const fail = (error: Error) =>
    Effect.sync(() => {
      const current = Array.from(subscribers.keys())
      subscribers.clear()
      for (const subscriber of current) Queue.failCauseUnsafe(subscriber, Cause.fail(error))
    })

  const publish = Effect.fnUntraced(function* (event: Event.Payload) {
    const space = isKVEvent(event) ? event.data.space : undefined
    if (!isOpenCodeEvent(event)) return
    let encoded: string | undefined
    for (const [subscriber, filter] of subscribers) {
      if (space === undefined ? filter.types && !filter.types.has(event.type) : !filter.spaces.has(space)) continue
      encoded ??= yield* Effect.try({
        try: () => render(event),
        catch: (cause) => new EncodingError({ eventID: event.id, eventType: event.type, cause }),
      }).pipe(
        Effect.catch((error) =>
          Effect.logError("Failed to encode public event", {
            eventID: error.eventID,
            eventType: error.eventType,
            cause: error.cause,
          }).pipe(Effect.andThen(fail(error)), Effect.as(undefined)),
        ),
      )
      if (encoded === undefined) return
      if (Queue.offerUnsafe(subscriber, encoded)) continue
      subscribers.delete(subscriber)
      Queue.failCauseUnsafe(subscriber, Cause.fail(new SubscriberOverflowError({ capacity })))
    }
  })

  const unsubscribe = yield* observe(publish)
  yield* Effect.addFinalizer(() => unsubscribe)

  return Service.of({
    subscribe: (types, spaces) =>
      Effect.acquireRelease(
        Queue.dropping<string, Error>(capacity).pipe(
          Effect.tap((queue) =>
            Effect.sync(() =>
              subscribers.set(queue, {
                types: types?.length ? new Set(types) : undefined,
                spaces: new Set(spaces),
              }),
            ),
          ),
        ),
        (queue) =>
          Effect.sync(() => subscribers.delete(queue)).pipe(Effect.andThen(Queue.shutdown(queue)), Effect.asVoid),
      ).pipe(Effect.map(Stream.fromQueue)),
  })
})

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const bus = yield* Bus.Service
    return yield* make(bus.listen)
  }),
)
