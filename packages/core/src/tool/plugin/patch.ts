export * as PatchTool from "./patch.js"

import type { Context } from "@opencode/plugin/effect/plugin"
import type { SessionHooks } from "@opencode/plugin/effect/session"
import { ToolFailure } from "@opencode/ai/effect"
import { FileDiff } from "@opencode/schema/file-diff"
import { Effect, Result, Schema } from "effect"
import { Bom } from "@opencode/util/bom"
import { toolFailure } from "../runtime.js"
import { Config } from "../../config.js"
import { Environment } from "../../environment/index.js"
import { Formatter } from "../../formatter.js"
import { FileMutation } from "../../file-mutation.js"
import { Location } from "../../location.js"
import { FileAccess } from "../../file-access.js"
import { Patch } from "@opencode/util/patch"
import type { ConfigExperimental } from "@opencode/schema/config/experimental"
import type { Document, Entry } from "@opencode/schema/config"
import { Permission } from "../../permission.js"
import { Session } from "../../session.js"
import DESCRIPTION from "../patch.txt"
import GRAMMAR from "../patch-grammar.txt"
import { fileDiff } from "./file-diff.js"

export const name = "patch"

export const Input = Schema.Struct({
  patchText: Schema.String.annotate({
    description: "The full patch text describing add, update, and delete operations",
  }),
})

export const Output = Schema.Record(Schema.String, Schema.Never)

type Prepared =
  | (Extract<Patch.Hunk, { readonly type: "add" }> & {
      readonly target: FileAccess.Target
      readonly content: string
      readonly before: string
      readonly after: string
    })
  | (Extract<Patch.Hunk, { readonly type: "delete" }> & {
      readonly target: FileAccess.Target
      readonly before: string
      readonly after: string
    })
  | (Extract<Patch.Hunk, { readonly type: "update" }> & {
      readonly target: FileAccess.Target
      readonly content: string
      readonly before: string
      readonly after: string
      readonly moveTarget?: FileAccess.Target
    })

export const Plugin = {
  id: "opencode.tool.patch",
  effect: Effect.fn("PatchTool.Plugin")(function* (ctx: Context) {
    const config = yield* Config.Service
    const sessions = yield* Session.Service
    const environment = yield* Environment.Service
    const access = yield* FileAccess.Service
    const fileMutation = yield* FileMutation.Service
    const formatter = yield* Formatter.Service
    const location = yield* Location.Service
    const permission = yield* Permission.Service

    yield* ctx.tool
      .transform((editor) =>
        editor.add({
          name,
          options: { codemode: false, permission: "edit" },
          description: DESCRIPTION,
          input: Input,
          freeform: { type: "lark", grammar: GRAMMAR },
          output: Output,
          execute: (input, context) => {
            const completed: string[] = []
            const parsed = Patch.parse(input.patchText)
            const lockTargets = Result.isSuccess(parsed)
              ? parsed.success.flatMap((hunk) => [
                  FileAccess.resolvePath(location.directory, hunk.path),
                  ...(hunk.type === "update" && hunk.movePath
                    ? [FileAccess.resolvePath(location.directory, hunk.movePath)]
                    : []),
                ])
              : []
            const fail = (operation: string, error: unknown) => {
              return new ToolFailure({
                message: `${operation}: ${errorMessage(error)}${completed.length ? `. Completed before failure: ${completed.join(", ")}` : ""}`,
              })
            }
            return Effect.gen(function* () {
              if (!input.patchText) return yield* new ToolFailure({ message: "patchText is required" })
              const hunks = yield* Effect.fromResult(parsed).pipe(
                Effect.mapError((error) => new ToolFailure({ message: `patch verification failed: ${error.message}` })),
              )
              if (hunks.length === 0) {
                return yield* new ToolFailure({ message: "patch rejected: empty patch" })
              }
              const prepared: Prepared[] = []
              const updates = new Map<string, string | null>()
              const resolveTarget = Effect.fnUntraced(function* (value: string) {
                const target = yield* access.resolve({ path: value, kind: "file" })
                if (!target.externalDirectory) return target
                yield* access.authorizeExternal([target], context, {
                  filepath: target.absolute,
                  parentDir: target.externalDirectory.directory,
                })
                return target
              })
              for (const hunk of hunks) {
                yield* Effect.gen(function* () {
                  const target = yield* resolveTarget(hunk.path)
                  if (hunk.type === "add") {
                    const content =
                      hunk.contents.endsWith("\n") || hunk.contents === "" ? hunk.contents : `${hunk.contents}\n`
                    prepared.push({
                      ...hunk,
                      target,
                      content,
                      before: "",
                      after: Bom.split(content).text,
                    })
                    updates.set(target.absolute, content)
                    return
                  }
                  if (hunk.type === "delete") {
                    const previous = updates.get(target.absolute)
                    if (previous === null) {
                      return yield* new ToolFailure({
                        message: `patch verification failed: Failed to delete ${target.resource}: file does not exist`,
                      })
                    }
                    const before =
                      previous !== undefined
                        ? Bom.split(previous).text
                        : (yield* FileMutation.readText(environment.files, target.absolute).pipe(
                            Effect.mapError(
                              (error) =>
                                new ToolFailure({
                                  message: `patch verification failed: Failed to delete ${target.resource}: ${errorMessage(error)}`,
                                }),
                            ),
                          )).text
                    prepared.push({ ...hunk, target, before, after: "" })
                    updates.set(target.absolute, null)
                    return
                  }
                  const previous = updates.get(target.absolute)
                  if (previous === null) {
                    return yield* new ToolFailure({
                      message: `patch verification failed: Failed to read file to update ${target.absolute}: file does not exist`,
                    })
                  }
                  const original =
                    previous ??
                    (yield* Effect.gen(function* () {
                      const content = yield* FileMutation.readText(environment.files, target.absolute).pipe(
                        Effect.mapError(
                          (error) =>
                            new ToolFailure({
                              message: `patch verification failed: Failed to read file to update ${target.absolute}: ${errorMessage(error)}`,
                            }),
                        ),
                      )
                      return Bom.join(content.text, content.bom)
                    }))
                  const before = Bom.split(original).text
                  const update = yield* Effect.try({
                    try: () => Patch.derive(hunk.path, hunk.chunks, original),
                    catch: (error) => new ToolFailure({ message: `patch verification failed: ${errorMessage(error)}` }),
                  })
                  const move = hunk.movePath ? yield* resolveTarget(hunk.movePath) : undefined
                  const moveTarget = move && move.absolute !== target.absolute ? move : undefined
                  const next = Patch.joinBom(update.content, update.bom)
                  prepared.push({
                    ...hunk,
                    target,
                    content: next,
                    before,
                    after: update.content,
                    moveTarget,
                  })
                  if (moveTarget) {
                    updates.set(target.absolute, null)
                    updates.set(moveTarget.absolute, next)
                  } else {
                    updates.set(target.absolute, next)
                  }
                }).pipe(Effect.mapError(toolFailure(`Unable to prepare patch at ${hunk.path}`)))
              }

              const patchFiles = prepared.map((change) => patchFile(change))
              const targets = prepared.flatMap((change) => [
                change.target,
                ...(change.type === "update" && change.moveTarget ? [change.moveTarget] : []),
              ])
              yield* permission.assert({
                action: "edit",
                resources: [...new Set(targets.map((target) => target.resource))],
                save: ["*"],
                metadata: {
                  filepath: targets.map((target) => target.resource).join(", "),
                  diff: patchFiles.map((file) => `${file.patch}\n`).join(""),
                  files: patchFiles,
                },
                ...Permission.invocation(context),
              })

              yield* Effect.forEach(
                prepared,
                (change) =>
                  Effect.gen(function* () {
                    const moveTarget = change.type === "update" ? change.moveTarget : undefined
                    const target = moveTarget ?? change.target
                    // On macOS and Windows a move that only changes case targets the same file, so the new name already
                    // exists, and writing it then removing the old one would delete the file. Such a move writes in
                    // place and renames. Symlinks keep the write-and-remove path, which leaves their target alone.
                    const recased =
                      moveTarget !== undefined &&
                      moveTarget.absolute.toLowerCase() === change.target.absolute.toLowerCase() &&
                      (yield* environment.files.stat(change.target.absolute).pipe(
                        Effect.map((info) => info.type === "file"),
                        Effect.orElseSucceed(() => false),
                      )) &&
                      (yield* environment.files.stat(moveTarget.absolute).pipe(
                        Effect.as(true),
                        Effect.orElseSucceed(() => false),
                      ))
                    if (change.type === "delete")
                      yield* environment.files
                        .remove(change.target.absolute)
                        .pipe(Effect.mapError((error) => fail(`Failed to delete ${change.target.resource}`, error)))
                    if (change.type !== "delete")
                      yield* environment.files
                        .write(
                          recased ? change.target.absolute : target.absolute,
                          new TextEncoder().encode(change.content),
                        )
                        .pipe(Effect.mapError((error) => fail(`Failed to write ${target.resource}`, error)))
                    if (moveTarget && recased)
                      yield* environment.files
                        .move(change.target.absolute, moveTarget.absolute)
                        .pipe(Effect.mapError((error) => fail(`Failed to rename ${change.target.resource}`, error)))
                    if (moveTarget && !recased)
                      yield* environment.files
                        .remove(change.target.absolute)
                        .pipe(
                          Effect.mapError((error) =>
                            fail(`Wrote ${moveTarget.resource} but failed to remove ${change.target.resource}`, error),
                          ),
                        )
                    completed.push(target.resource)
                  }),
                { discard: true },
              )
              const formatted = new Map<string, string>()
              yield* Effect.forEach(
                [...updates.entries()].flatMap(([target, value]) => (value === null ? [] : [target])),
                (target) =>
                  Effect.gen(function* () {
                    const current = yield* FileMutation.readText(environment.files, target).pipe(
                      Effect.mapError((error) => fail(`Failed to read ${target}`, error)),
                    )
                    formatted.set(
                      target,
                      (yield* formatter.file(target))
                        ? yield* FileMutation.syncTextBom(environment.files, target, current.bom).pipe(
                            Effect.mapError((error) => fail(`Failed to sync ${target}`, error)),
                          )
                        : current.text,
                    )
                  }),
                { discard: true },
              )
              const files = prepared.map((change) => {
                if (change.type === "delete") return patchFile(change)
                const target = change.type === "update" && change.moveTarget ? change.moveTarget : change.target
                return patchFile(change, formatted.get(target.absolute))
              })
              const session = yield* sessions.get(context.sessionID).pipe(Effect.orElseSucceed(() => undefined))
              const resultStyle = selectEditingResult(
                session?.model,
                configuredEditing(yield* config.entries()),
              )
              return {
                // The multi-file patch contract intentionally leaves structured output empty.
                output: {},
                content: renderResult(files, resultStyle),
                metadata: { files },
              }
            }).pipe(fileMutation.withLock(lockTargets), Effect.mapError(toolFailure("Unable to apply patch")))
          },
        }),
      )
      .pipe(Effect.orDie)

    const hook = (event: SessionHooks["context"]) =>
      Effect.gen(function* () {
        const entries = yield* config.entries()
        const tools = selectEditingTools(event.model, configuredEditing(entries))
        for (const name of EDITING_TOOLS) {
          if (!tools.has(name)) delete event.tools[name]
        }
      })
    yield* ctx.session.hook("context", hook)
    yield* ctx.session.hook("compaction", hook)
    yield* ctx.session.hook("generate", hook)
  }),
}

export const EDITING_TOOLS = ["patch", "edit", "write"] as const
type EditingTool = (typeof EDITING_TOOLS)[number]
type ModelRef = SessionHooks["context"]["model"]

function heuristicEditingTools(model: ModelRef): ReadonlySet<EditingTool> {
  const patch = model.id.includes("gpt-") && !model.id.includes("oss") && !model.id.includes("gpt-4")
  return new Set<EditingTool>(patch ? ["patch"] : ["edit", "write"])
}

function matchingRule(model: ModelRef | undefined, editing: ConfigExperimental.Editing | undefined) {
  if (!model || !editing) return undefined
  return editing.rules?.findLast((candidate) =>
    new RegExp(candidate.match).test(`${model.providerID}/${model.id}`),
  )
}

function selectEditingTools(model: ModelRef, editing: ConfigExperimental.Editing | undefined) {
  const tools = new Set(heuristicEditingTools(model))
  if (!editing) return tools
  const delta = matchingRule(model, editing) ?? editing
  for (const name of delta.add ?? []) tools.add(name)
  for (const name of delta.remove ?? []) tools.delete(name)
  return tools
}

function selectEditingResult(
  model: ModelRef | undefined,
  editing: ConfigExperimental.Editing | undefined,
): ConfigExperimental.EditingResult {
  if (!editing) return "summary"
  return matchingRule(model, editing)?.result_style ?? editing.result_style ?? "summary"
}

function renderResult(
  files: ReadonlyArray<typeof FileDiff.Info.Type>,
  style: ConfigExperimental.EditingResult,
) {
  const label = (file: (typeof files)[number]) =>
    `${file.status === "added" ? "A" : file.status === "deleted" ? "D" : "M"} ${file.file}${
      style === "summary" ? "" : ` (+${file.additions} -${file.deletions})`
    }`
  return [
    "Success. Updated the following files:",
    ...files.flatMap((file) => (style === "diff" ? [`${label(file)}\n${renderDiff(file.patch)}`] : [label(file)])),
  ].join("\n")
}

function renderDiff(patch: string) {
  const lines = patch.split("\n")
  const start = lines.findIndex((line) => line.startsWith("@@"))
  if (start === -1) return ""
  const body = lines.slice(start)
  const limited = body.slice(0, 200)
  const text = limited.join("\n")
  const clipped = text.length > 12_000 ? text.slice(0, 12_000) : text
  return body.length > limited.length || text.length > clipped.length ? `${clipped}\n... diff truncated ...` : clipped
}

function errorMessage(error: unknown) {
  if (error instanceof Environment.NotFound) return "file does not exist"
  if (error instanceof Environment.WrongKind)
    return error.actual === "directory" ? "path is a directory" : `path is ${error.actual}`
  if (error instanceof Environment.Failed) return errorMessage(error.cause)
  return error instanceof Error ? error.message : String(error)
}

// Read per field so a later config that sets other experimental options doesn't hide this one.
function configuredEditing(entries: readonly Entry[]) {
  return entries.findLast(
    (entry): entry is Document => entry.type === "document" && entry.info.experimental?.editing !== undefined,
  )?.info.experimental?.editing
}

function patchFile(change: Prepared, after = change.after): typeof FileDiff.Info.Type {
  const target = (change.type === "update" ? change.moveTarget : undefined)?.resource ?? change.target.resource
  const diff = fileDiff(
    change.target.absolute,
    change.before,
    after,
    change.type === "add" ? "added" : change.type === "delete" ? "deleted" : "modified",
  )
  return {
    ...diff,
    file: target,
    patch: trimDiff(diff.patch),
  }
}

function trimDiff(diff: string) {
  const lines = diff.split("\n")
  const content = lines.filter(
    (line) =>
      (line.startsWith("+") || line.startsWith("-") || line.startsWith(" ")) &&
      !line.startsWith("---") &&
      !line.startsWith("+++"),
  )
  if (content.length === 0) return diff
  const indent = content.reduce((result, line) => {
    const value = line.slice(1)
    if (value.trim().length === 0) return result
    return Math.min(result, value.match(/^(\s*)/)?.[1].length ?? result)
  }, Infinity)
  if (indent === Infinity || indent === 0) return diff
  return lines
    .map((line) => {
      if (
        (line.startsWith("+") || line.startsWith("-") || line.startsWith(" ")) &&
        !line.startsWith("---") &&
        !line.startsWith("+++")
      ) {
        return line[0] + line.slice(1 + indent)
      }
      return line
    })
    .join("\n")
}
