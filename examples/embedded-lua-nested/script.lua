-- script.lua — deterministic Lua program driven by the native host.
--
-- This is the "nested" program: it runs inside the embedded Lua VM while the
-- native C host records a source-level materialized trace of it through the
-- VM's OWN per-line hook (lua_sethook / LUA_MASKLINE|CALL|RET). NO Lua fork.
--
-- It exercises: per-line steps, Lua->Lua calls/returns, scalar locals
-- (int/float/string/bool) for value capture, and two Lua->host callbacks
-- (host_note) — the native-call crossings where the native host trace is the
-- continuation of a Lua source step (the host<->VM join boundary).
--
-- Fully deterministic: fixed inputs, fixed control flow, fixed result.

local function add(a, b)
	local sum = a + b
	return sum
end

local function scale(x)
	local factor = 3
	local scaled = x * factor
	return scaled
end

local function greet(name)
	local prefix = "hi "
	local msg = prefix .. name
	return msg
end

local total = 0
for i = 1, 3 do
	total = add(total, i)
end

local ratio = 1.5
local scaled = scale(total)
local flag = scaled > 10
local who = greet("lua")

-- Lua -> host callbacks: the native-call join sites.
host_note("total", total)
host_note("scaled", scaled)

return scaled
