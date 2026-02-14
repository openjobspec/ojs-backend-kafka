package state

import "github.com/redis/go-redis/v9"

// Lua scripts for atomic multi-step Redis operations.
// These prevent partial state from crashes between individual commands.

// pushJobScript atomically: HSET job, ZADD to available/scheduled, SADD to queues.
// KEYS[1] = job hash key
// KEYS[2] = queue available sorted set key OR scheduled sorted set key
// KEYS[3] = queues set key
// ARGV[1..N] = job hash field/value pairs (alternating)
// ARGV[N+1] = "__score__" sentinel
// ARGV[N+2] = score value
// ARGV[N+3] = job ID (member for sorted set)
// ARGV[N+4] = queue name (for SADD)
var pushJobScript = redis.NewScript(`
local jobKey = KEYS[1]
local sortedSetKey = KEYS[2]
local queuesKey = KEYS[3]

-- Parse field/value pairs until sentinel
local fields = {}
local scoreIdx = 0
for i = 1, #ARGV do
    if ARGV[i] == "__score__" then
        scoreIdx = i
        break
    end
    fields[#fields + 1] = ARGV[i]
end

-- Set job hash
if #fields > 0 then
    redis.call('HSET', jobKey, unpack(fields))
end

-- Add to sorted set (available or scheduled)
local score = tonumber(ARGV[scoreIdx + 1])
local jobID = ARGV[scoreIdx + 2]
local queueName = ARGV[scoreIdx + 3]

redis.call('ZADD', sortedSetKey, score, jobID)
redis.call('SADD', queuesKey, queueName)

return 1
`)

// ackJobScript atomically: HSET state=completed, SREM from active, DEL visibility, INCR completed.
// KEYS[1] = job hash key
// KEYS[2] = queue active set key
// KEYS[3] = visibility key
// KEYS[4] = completed counter key
// ARGV[1] = job ID
// ARGV[2] = completed_at timestamp
// ARGV[3] = result (may be empty)
var ackJobScript = redis.NewScript(`
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local visKey = KEYS[3]
local completedKey = KEYS[4]
local jobID = ARGV[1]
local completedAt = ARGV[2]
local result = ARGV[3]

redis.call('HSET', jobKey, 'state', 'completed', 'completed_at', completedAt)
if result ~= '' then
    redis.call('HSET', jobKey, 'result', result)
end
redis.call('HDEL', jobKey, 'error')
redis.call('SREM', activeKey, jobID)
redis.call('DEL', visKey)
redis.call('INCR', completedKey)

return 1
`)

// fetchJobScript atomically: ZPOPMIN from available, HSET state=active, SADD to active, SET visibility.
// KEYS[1] = queue available sorted set key
// KEYS[2] = queue active set key
// ARGV[1] = visibility deadline string
// Returns: job ID or empty string
var fetchJobScript = redis.NewScript(`
local availableKey = KEYS[1]
local activeKey = KEYS[2]
local visDeadline = ARGV[1]

-- Pop lowest-scored job
local results = redis.call('ZPOPMIN', availableKey, 1)
if #results == 0 then
    return ''
end

local jobID = results[1]

-- Activate
redis.call('SADD', activeKey, jobID)

-- Set visibility
local visKey = 'ojs:visibility:' .. jobID
redis.call('SET', visKey, visDeadline)

return jobID
`)

// nackDiscardScript atomically: HSET state=discarded, SREM active, DEL visibility, optionally ZADD dead.
// KEYS[1] = job hash key
// KEYS[2] = queue active set key
// KEYS[3] = visibility key
// KEYS[4] = dead letter sorted set key (may be "")
// ARGV[1] = job ID
// ARGV[2] = completed_at
// ARGV[3] = error JSON
// ARGV[4] = error_history JSON
// ARGV[5] = attempt (string)
// ARGV[6] = "1" if add to dead letter, "0" otherwise
// ARGV[7] = nowMs for dead letter score
var nackDiscardScript = redis.NewScript(`
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local visKey = KEYS[3]
local deadKey = KEYS[4]
local jobID = ARGV[1]
local completedAt = ARGV[2]
local errJSON = ARGV[3]
local histJSON = ARGV[4]
local attempt = ARGV[5]
local addToDead = ARGV[6]
local nowMs = tonumber(ARGV[7])

redis.call('HSET', jobKey, 'state', 'discarded', 'completed_at', completedAt, 'error_history', histJSON, 'attempt', attempt)
if errJSON ~= '' then
    redis.call('HSET', jobKey, 'error', errJSON)
end
redis.call('SREM', activeKey, jobID)
redis.call('DEL', visKey)

if addToDead == '1' then
    redis.call('ZADD', deadKey, nowMs, jobID)
end

return 1
`)

// nackRetryScript atomically: HSET state=retryable, SREM active, DEL visibility, ZADD retry.
// KEYS[1] = job hash key
// KEYS[2] = queue active set key
// KEYS[3] = visibility key
// KEYS[4] = retry sorted set key
// ARGV[1] = job ID
// ARGV[2] = error JSON
// ARGV[3] = error_history JSON
// ARGV[4] = attempt (string)
// ARGV[5] = retry_delay_ms (string)
// ARGV[6] = retryAtMs (score for retry sorted set)
var nackRetryScript = redis.NewScript(`
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local visKey = KEYS[3]
local retryKey = KEYS[4]
local jobID = ARGV[1]
local errJSON = ARGV[2]
local histJSON = ARGV[3]
local attempt = ARGV[4]
local retryDelayMs = ARGV[5]
local retryAtMs = tonumber(ARGV[6])

redis.call('HSET', jobKey, 'state', 'retryable', 'error_history', histJSON, 'attempt', attempt, 'retry_delay_ms', retryDelayMs)
if errJSON ~= '' then
    redis.call('HSET', jobKey, 'error', errJSON)
end
redis.call('SREM', activeKey, jobID)
redis.call('DEL', visKey)
redis.call('ZADD', retryKey, retryAtMs, jobID)

return 1
`)
