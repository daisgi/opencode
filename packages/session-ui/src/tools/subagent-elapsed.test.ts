import { describe, expect, test } from "bun:test"
import { subagentElapsedTime } from "./subagent-elapsed"

const begin = 10_000

describe("subagentElapsedTime", () => {
  test("ticks from the call start while the child runs", () => {
    expect(subagentElapsedTime({ begin, running: true, childUpdated: undefined, completed: undefined })).toEqual({
      created: begin,
      ran: begin,
    })
  })

  test("freezes a background dispatch at the child session's last update, not the call's launch completion", () => {
    expect(
      subagentElapsedTime({
        begin,
        running: false,
        childUpdated: begin + 40_000,
        completed: begin + 4,
      }),
    ).toEqual({ created: begin, ran: begin, completed: begin + 40_000 })
  })

  test("keeps a blocking subagent's own completion", () => {
    expect(subagentElapsedTime({ begin, running: false, childUpdated: undefined, completed: begin + 50_000 })).toEqual({
      created: begin,
      ran: begin,
      completed: begin + 50_000,
    })
  })

  test("reports no time when nothing settled after the start instead of counting from the render clock", () => {
    expect(subagentElapsedTime({ begin, running: false, childUpdated: undefined, completed: begin })).toEqual({
      created: begin,
      ran: begin,
    })
  })

  test("has no time before the call starts", () => {
    expect(
      subagentElapsedTime({ begin: undefined, running: false, childUpdated: begin + 1, completed: undefined }),
    ).toBeUndefined()
  })
})
