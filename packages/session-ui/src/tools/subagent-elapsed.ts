/**
 * A background subagent dispatches by completing its tool call at launch, so the call's own `completed` is a few
 * milliseconds after `begin` and would freeze the row at ~0s. The child session's last update is the run's real end
 * once it settles, so it takes precedence; a blocking subagent that finishes inside the call still uses `completed`.
 * Nothing here reads the render clock: a session opened after the child finished shows the run's duration, not the
 * time since launch.
 */
export function subagentElapsedTime(input: {
  begin: number | undefined
  running: boolean
  childUpdated: number | undefined
  completed: number | undefined
}) {
  const begin = input.begin

  if (begin === undefined) return undefined

  if (input.running) return { created: begin, ran: begin }

  const end = Math.max(input.childUpdated ?? 0, input.completed ?? 0)

  // Neither the call nor a loaded child session settled after the start, so report no time rather than 0s.
  if (end <= begin) return { created: begin, ran: begin }

  return { created: begin, ran: begin, completed: end }
}
