when defined(nimPreviewSlimSystem):
  import std/[syncio, assertions]

{.push raises: [].}

## Mutation arms: each production-form rule a test must be able to catch has
## an arm that breaks it, selected at run time by `CTFS_KEYED_MUTATE`. The
## arms exist only in a build with `-d:ctfsKeyedMutation`, which a test passes
## and no production build does; elsewhere `mutated` is the constant `false`
## and every arm compiles away.

when defined(ctfsKeyedMutation):
  import std/os
  var keyedMutationCache {.threadvar.}: string
  var keyedMutationRead {.threadvar.}: bool
  var keyedMutationFired* {.threadvar.}: int
    ## How many times a mutation arm took its mutated path: a negative control
    ## asserts its subject ran.

  proc keyedMutation*(): string =
    ## The arm `CTFS_KEYED_MUTATE` names, read once.
    if not keyedMutationRead:
      keyedMutationRead = true
      try: keyedMutationCache = getEnv("CTFS_KEYED_MUTATE")
      except CatchableError: keyedMutationCache = ""
    keyedMutationCache

  proc mutated*(arm: string): bool =
    result = keyedMutation() == arm
    if result: inc keyedMutationFired
else:
  template mutated*(arm: string): bool = false
