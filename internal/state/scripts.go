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

// pushJobIfAbsentScript atomically creates and indexes a job only when its
// stable job hash does not already exist. Existing jobs are never rewritten or
// re-indexed, regardless of their current lifecycle state.
//
// KEYS[1] = job hash key
// KEYS[2] = queue available sorted set key OR scheduled sorted set key
// KEYS[3] = queues set key
// ARGV uses the same field/sentinel layout as pushJobScript.
var pushJobIfAbsentScript = redis.NewScript(`
local jobKey = KEYS[1]
local sortedSetKey = KEYS[2]
local queuesKey = KEYS[3]

if redis.call('EXISTS', jobKey) == 1 then
    return 0
end

local function keyType(key)
    return redis.call('TYPE', key).ok
end

local sortedType = keyType(sortedSetKey)
if sortedType ~= 'none' and sortedType ~= 'zset' then
    return redis.error_reply('job create-if-absent sorted set key must be a sorted set')
end
local queuesType = keyType(queuesKey)
if queuesType ~= 'none' and queuesType ~= 'set' then
    return redis.error_reply('job create-if-absent queues key must be a set')
end

local fields = {}
local scoreIdx = 0
for i = 1, #ARGV do
    if ARGV[i] == "__score__" then
        scoreIdx = i
        break
    end
    fields[#fields + 1] = ARGV[i]
end
if scoreIdx == 0 then
    return redis.error_reply('job create-if-absent score sentinel is missing')
end

local score = tonumber(ARGV[scoreIdx + 1])
local jobID = ARGV[scoreIdx + 2]
local queueName = ARGV[scoreIdx + 3]
if score == nil or jobID == '' or queueName == '' then
    return redis.error_reply('job create-if-absent arguments are invalid')
end

if #fields > 0 then
    redis.call('HSET', jobKey, unpack(fields))
end
redis.call('ZADD', sortedSetKey, score, jobID)
redis.call('SADD', queuesKey, queueName)
return 1
`)

// cancelJobScript atomically cancels a non-terminal job and removes it from
// every lifecycle index. All key types are validated before the first mutation.
//
// KEYS[1] = job hash key
// KEYS[2] = global scheduled sorted set
// KEYS[3] = global retry sorted set
// KEYS[4] = visibility key
// ARGV[1] = job ID
// ARGV[2] = cancelled_at timestamp
// Returns {cancelledFlag, previousState}.
var cancelJobScript = redis.NewScript(`
local jobKey = KEYS[1]
local scheduledKey = KEYS[2]
local retryKey = KEYS[3]
local visibilityKey = KEYS[4]
local jobID = ARGV[1]
local cancelledAt = ARGV[2]

local function keyType(key)
    return redis.call('TYPE', key).ok
end

local jobType = keyType(jobKey)
if jobType == 'none' then
    return {0, ''}
end
if jobType ~= 'hash' then
    return redis.error_reply('job cancellation job key must be a hash')
end

local state = redis.call('HGET', jobKey, 'state') or ''
if state == 'completed' or state == 'cancelled' or state == 'discarded' then
    return {0, state}
end
if state ~= 'available' and state ~= 'scheduled' and state ~= 'pending' and
   state ~= 'active' and state ~= 'retryable' then
    return redis.error_reply('job cancellation state is invalid')
end

local queue = redis.call('HGET', jobKey, 'queue') or ''
if queue == '' then
    return redis.error_reply('job cancellation queue is missing')
end
local availableKey = 'ojs:queue:' .. queue .. ':available'
local activeKey = 'ojs:queue:' .. queue .. ':active'

local availableType = keyType(availableKey)
if availableType ~= 'none' and availableType ~= 'zset' then
    return redis.error_reply('job cancellation available key must be a sorted set')
end
local activeType = keyType(activeKey)
if activeType ~= 'none' and activeType ~= 'set' then
    return redis.error_reply('job cancellation active key must be a set')
end
local scheduledType = keyType(scheduledKey)
if scheduledType ~= 'none' and scheduledType ~= 'zset' then
    return redis.error_reply('job cancellation scheduled key must be a sorted set')
end
local retryType = keyType(retryKey)
if retryType ~= 'none' and retryType ~= 'zset' then
    return redis.error_reply('job cancellation retry key must be a sorted set')
end
local visibilityType = keyType(visibilityKey)
if visibilityType ~= 'none' and visibilityType ~= 'string' then
    return redis.error_reply('job cancellation visibility key must be a string')
end

redis.call('HSET', jobKey,
    'state', 'cancelled',
    'cancelled_at', cancelledAt)
redis.call('ZREM', availableKey, jobID)
redis.call('SREM', activeKey, jobID)
redis.call('ZREM', scheduledKey, jobID)
redis.call('ZREM', retryKey, jobID)
redis.call('DEL', visibilityKey)
return {1, state}
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

// requeueJobScript atomically returns an active job to its available queue.
// It validates every key type before mutating so a Redis WRONGTYPE error cannot
// leave a partially-applied transition.
// KEYS[1] = job hash key
// KEYS[2] = queue active set key
// KEYS[3] = visibility key
// KEYS[4] = queue available sorted set key
// ARGV[1] = job ID
// ARGV[2] = enqueued_at timestamp
// ARGV[3] = available score
var requeueJobScript = redis.NewScript(`
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local visKey = KEYS[3]
local availableKey = KEYS[4]
local jobID = ARGV[1]
local enqueuedAt = ARGV[2]
local score = tonumber(ARGV[3])

local function keyType(key)
    return redis.call('TYPE', key).ok
end

if keyType(jobKey) ~= 'hash' then
    return redis.error_reply('job requeue job key must be a hash')
end
local activeType = keyType(activeKey)
if activeType ~= 'none' and activeType ~= 'set' then
    return redis.error_reply('job requeue active key must be a set')
end
local visType = keyType(visKey)
if visType ~= 'none' and visType ~= 'string' then
    return redis.error_reply('job requeue visibility key must be a string')
end
local availableType = keyType(availableKey)
if availableType ~= 'none' and availableType ~= 'zset' then
    return redis.error_reply('job requeue available key must be a sorted set')
end
if score == nil then
    return redis.error_reply('job requeue score must be numeric')
end
if redis.call('HGET', jobKey, 'state') ~= 'active' then
    return 0
end

redis.call('HSET', jobKey,
    'state', 'available',
    'started_at', '',
    'worker_id', '',
    'enqueued_at', enqueuedAt)
redis.call('SREM', activeKey, jobID)
redis.call('DEL', visKey)
redis.call('ZADD', availableKey, score, jobID)

return 1
`)

// fetchJobScript atomically claims the first actually-available job, transitions
// its hash to active, adds it to the active set, and sets visibility. Stale
// available-index entries are discarded without resurrecting cancelled jobs.
// KEYS[1] = queue available sorted set key
// KEYS[2] = queue active set key
// ARGV[1] = visibility deadline string
// ARGV[2] = started_at timestamp
// ARGV[3] = worker ID
// Returns: job ID or empty string
var fetchJobScript = redis.NewScript(`
local availableKey = KEYS[1]
local activeKey = KEYS[2]
local visDeadline = ARGV[1]
local startedAt = ARGV[2]
local workerID = ARGV[3]

local function keyType(key)
    return redis.call('TYPE', key).ok
end

local availableType = keyType(availableKey)
if availableType ~= 'none' and availableType ~= 'zset' then
    return redis.error_reply('job fetch available key must be a sorted set')
end
local activeType = keyType(activeKey)
if activeType ~= 'none' and activeType ~= 'set' then
    return redis.error_reply('job fetch active key must be a set')
end

while true do
    local results = redis.call('ZRANGE', availableKey, 0, 0)
    if #results == 0 then
        return ''
    end
    local jobID = results[1]
    local jobKey = 'ojs:job:' .. jobID
    local jobType = keyType(jobKey)
    if jobType == 'none' then
        redis.call('ZREM', availableKey, jobID)
    elseif jobType ~= 'hash' then
        return redis.error_reply('job fetch job key must be a hash')
    else
        local state = redis.call('HGET', jobKey, 'state') or ''
        if state ~= 'available' then
            redis.call('ZREM', availableKey, jobID)
        else
            local visKey = 'ojs:visibility:' .. jobID
            local visType = keyType(visKey)
            if visType ~= 'none' and visType ~= 'string' then
                return redis.error_reply('job fetch visibility key must be a string')
            end

            redis.call('ZREM', availableKey, jobID)
            redis.call('HSET', jobKey,
                'state', 'active',
                'started_at', startedAt,
                'worker_id', workerID)
            redis.call('SADD', activeKey, jobID)
            redis.call('SET', visKey, visDeadline)

            return jobID
        end
    end
end
`)

// promoteJobScript atomically moves a scheduled or retryable job to available
// only if its hash still has the expected source state. A concurrent
// cancellation therefore wins cleanly instead of being overwritten.
//
// KEYS[1] = job hash
// KEYS[2] = scheduled or retry sorted set
// KEYS[3] = queue available sorted set
// ARGV[1] = job ID
// ARGV[2] = expected source state
// ARGV[3] = enqueued_at timestamp
// ARGV[4] = available score
var promoteJobScript = redis.NewScript(`
local jobKey = KEYS[1]
local sourceKey = KEYS[2]
local availableKey = KEYS[3]
local jobID = ARGV[1]
local expectedState = ARGV[2]
local enqueuedAt = ARGV[3]
local score = tonumber(ARGV[4])

local function keyType(key)
    return redis.call('TYPE', key).ok
end

if keyType(jobKey) ~= 'hash' then
    return redis.error_reply('job promotion job key must be a hash')
end
local sourceType = keyType(sourceKey)
if sourceType ~= 'none' and sourceType ~= 'zset' then
    return redis.error_reply('job promotion source key must be a sorted set')
end
local availableType = keyType(availableKey)
if availableType ~= 'none' and availableType ~= 'zset' then
    return redis.error_reply('job promotion available key must be a sorted set')
end
if expectedState ~= 'scheduled' and expectedState ~= 'retryable' then
    return redis.error_reply('job promotion source state is invalid')
end
if score == nil then
    return redis.error_reply('job promotion score must be numeric')
end
if redis.call('HGET', jobKey, 'state') ~= expectedState then
    return 0
end
if redis.call('ZSCORE', sourceKey, jobID) == false then
    return 0
end

redis.call('HSET', jobKey,
    'state', 'available',
    'enqueued_at', enqueuedAt)
redis.call('ZREM', sourceKey, jobID)
redis.call('ZADD', availableKey, score, jobID)
return 1
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

// claimCronOccurrenceScript acquires a lease for one scheduled occurrence.
// A stale pending claim is reconciled against the claimed job hash: if the job
// exists, the occurrence is finalized instead of being enqueued again.
// KEYS[1] = occurrence marker hash
// ARGV[1] = owner token
// ARGV[2] = proposed job ID
// ARGV[3] = current time in milliseconds
// ARGV[4] = lease duration in milliseconds
// ARGV[5] = marker retention in milliseconds
var claimCronOccurrenceScript = redis.NewScript(`
local markerKey = KEYS[1]
local owner = ARGV[1]
local proposedJobID = ARGV[2]
local nowMs = tonumber(ARGV[3])
local leaseMs = tonumber(ARGV[4])
local retentionMs = tonumber(ARGV[5])
local markerType = redis.call('TYPE', markerKey).ok

if markerType ~= 'none' and markerType ~= 'hash' then
    return redis.error_reply('cron occurrence marker must be a hash')
end
if nowMs == nil or leaseMs == nil or leaseMs <= 0 or
   retentionMs == nil or retentionMs <= leaseMs then
    return redis.error_reply('cron occurrence lease arguments are invalid')
end

if markerType == 'hash' then
    local state = redis.call('HGET', markerKey, 'state')
    local existingJobID = redis.call('HGET', markerKey, 'job_id') or ''
    if state == 'fired' then
        redis.call('PEXPIRE', markerKey, retentionMs)
        return {'fired', existingJobID}
    end
    if state ~= 'pending' then
        return redis.error_reply('cron occurrence marker has an invalid state')
    end

    local leaseUntil = tonumber(redis.call('HGET', markerKey, 'lease_until'))
    if leaseUntil == nil then
        return redis.error_reply('cron occurrence marker has an invalid lease')
    end
    if leaseUntil > nowMs then
        redis.call('PEXPIRE', markerKey, retentionMs)
        return {'busy', existingJobID}
    end

    if existingJobID ~= '' and redis.call('EXISTS', 'ojs:job:' .. existingJobID) == 1 then
        redis.call('HSET', markerKey, 'state', 'fired')
        redis.call('HDEL', markerKey, 'owner', 'lease_until')
        redis.call('PEXPIRE', markerKey, retentionMs)
        return {'fired', existingJobID}
    end
end

redis.call('HSET', markerKey,
    'state', 'pending',
    'owner', owner,
    'job_id', proposedJobID,
    'lease_until', nowMs + leaseMs)
redis.call('PEXPIRE', markerKey, retentionMs)
return {'acquired', proposedJobID}
`)

// completeCronOccurrenceScript marks an occurrence as fired until its cursor
// persistence either deletes the marker or the bounded retention expires.
var completeCronOccurrenceScript = redis.NewScript(`
local markerKey = KEYS[1]
local owner = ARGV[1]
local jobID = ARGV[2]
local retentionMs = tonumber(ARGV[3])
local markerType = redis.call('TYPE', markerKey).ok

if markerType ~= 'hash' then
    return redis.error_reply('cron occurrence marker is missing')
end
if retentionMs == nil or retentionMs <= 0 then
    return redis.error_reply('cron occurrence retention must be positive')
end

local state = redis.call('HGET', markerKey, 'state')
local existingJobID = redis.call('HGET', markerKey, 'job_id') or ''
if state == 'fired' and existingJobID == jobID then
    redis.call('PEXPIRE', markerKey, retentionMs)
    return 1
end
if state ~= 'pending' or
   redis.call('HGET', markerKey, 'owner') ~= owner or
   existingJobID ~= jobID then
    return redis.error_reply('cron occurrence claim is not owned by caller')
end

redis.call('HSET', markerKey, 'state', 'fired')
redis.call('HDEL', markerKey, 'owner', 'lease_until')
redis.call('PEXPIRE', markerKey, retentionMs)
return 1
`)

// releaseCronOccurrenceScript releases the caller's pending claim or removes a
// fired marker after its cursor has been durably persisted.
var releaseCronOccurrenceScript = redis.NewScript(`
local markerKey = KEYS[1]
local owner = ARGV[1]
local jobID = ARGV[2]
local markerType = redis.call('TYPE', markerKey).ok

if markerType == 'none' then
    return 0
end
if markerType ~= 'hash' then
    return redis.error_reply('cron occurrence marker must be a hash')
end
local state = redis.call('HGET', markerKey, 'state')
local existingJobID = redis.call('HGET', markerKey, 'job_id') or ''
if existingJobID ~= jobID then
    return 0
end
if state == 'fired' or
   (state == 'pending' and redis.call('HGET', markerKey, 'owner') == owner) then
    return redis.call('DEL', markerKey)
end
return 0
`)

// claimUniqueJobScript atomically claims a unique fingerprint and creates the
// job in the same transaction, so a unique claim can never dangle without its
// job and two racing enqueues can never both win the same fingerprint.
//
// It inspects the existing claim (if any) and the relevance of the job it
// points to, then applies the conflict policy:
//   - reject: return {"rejected", existingID} without writing anything.
//   - ignore: return {"ignored", existingID} without writing anything.
//
// Replacement is intentionally handled by replaceUniqueJobScript, which first
// compare-and-cancels the predecessor and only then creates the replacement.
//
// A missing/terminal/filtered (non-relevant) existing claim is treated as
// stale: the new job simply claims it and {"claimed", ""} is returned.
//
// KEYS[1] = unique fingerprint key
// KEYS[2] = new job hash key
// KEYS[3] = available or scheduled sorted set key
// KEYS[4] = queues set key
// ARGV[1] = conflict action ("reject" | "ignore" | "replace")
// ARGV[2] = relevant states, comma-joined ("" = relevant when non-terminal)
// ARGV[3] = claim TTL in milliseconds (0 = no expiry)
// ARGV[4] = sorted set score
// ARGV[5] = new job ID
// ARGV[6] = queue name
// ARGV[7..] = new job hash field/value pairs (alternating)
var claimUniqueJobScript = redis.NewScript(`
local uniqueKey = KEYS[1]
local jobKey = KEYS[2]
local sortedSetKey = KEYS[3]
local queuesKey = KEYS[4]

local conflict = ARGV[1]
local relevantStates = ARGV[2]
local ttlMs = tonumber(ARGV[3])
local score = tonumber(ARGV[4])
local jobID = ARGV[5]
local queueName = ARGV[6]

if conflict == 'replace' then
    return redis.error_reply('unique replacement requires compare-and-cancel')
end

local function keyType(key)
    return redis.call('TYPE', key).ok
end

local uniqueType = keyType(uniqueKey)
if uniqueType ~= 'none' and uniqueType ~= 'string' then
    return redis.error_reply('unique claim key must be a string')
end
if keyType(jobKey) ~= 'none' and keyType(jobKey) ~= 'hash' then
    return redis.error_reply('unique job key must be a hash')
end
local sortedType = keyType(sortedSetKey)
if sortedType ~= 'none' and sortedType ~= 'zset' then
    return redis.error_reply('unique sorted set key must be a sorted set')
end
local queuesType = keyType(queuesKey)
if queuesType ~= 'none' and queuesType ~= 'set' then
    return redis.error_reply('unique queues key must be a set')
end
if score == nil or ttlMs == nil then
    return redis.error_reply('unique claim arguments are invalid')
end

local terminalStates = {completed = true, cancelled = true, discarded = true}
local function isRelevant(state)
    if state == false or state == nil or state == '' then
        return false
    end
    if relevantStates ~= '' then
        for item in string.gmatch(relevantStates, '([^,]+)') do
            if item == state then
                return true
            end
        end
        return false
    end
    return not terminalStates[state]
end

local replacedID = ''
local existing = redis.call('GET', uniqueKey)
if existing ~= false and existing ~= '' and existing ~= jobID then
    local existingState = redis.call('HGET', 'ojs:job:' .. existing, 'state')
    if isRelevant(existingState) then
        if conflict == 'reject' then
            return {'rejected', existing}
        elseif conflict == 'ignore' then
            return {'ignored', existing}
        else
            return redis.error_reply('unique replacement requires compare-and-cancel')
        end
    end
end

-- Claim the fingerprint and create the job atomically.
local fields = {}
for i = 7, #ARGV do
    fields[#fields + 1] = ARGV[i]
end
if #fields > 0 then
    redis.call('HSET', jobKey, unpack(fields))
end
redis.call('ZADD', sortedSetKey, score, jobID)
redis.call('SADD', queuesKey, queueName)
if ttlMs > 0 then
    redis.call('SET', uniqueKey, jobID, 'PX', ttlMs)
else
    redis.call('SET', uniqueKey, jobID)
end

return {'claimed', replacedID}
`)

// replaceUniqueJobScript performs a compare-and-replace for a unique job. The
// caller first reads the current claim and passes it as expectedID. The script
// then validates all keys, rejects an active/unsafe predecessor, atomically
// cancels and removes an available/scheduled/retryable predecessor, and only
// then creates and claims the replacement. Racing replacers that observed the
// same predecessor cannot both succeed because only the first still matches.
//
// KEYS[1] = unique fingerprint key
// KEYS[2] = replacement job hash key
// KEYS[3] = replacement available or scheduled sorted set key
// KEYS[4] = queues set key
// KEYS[5] = global scheduled sorted set
// KEYS[6] = global retry sorted set
// ARGV[1] = expected current job ID (empty when no claim was observed)
// ARGV[2] = relevant states, comma-joined (empty = any non-terminal state)
// ARGV[3] = claim TTL in milliseconds
// ARGV[4] = replacement score
// ARGV[5] = replacement job ID
// ARGV[6] = replacement queue
// ARGV[7] = predecessor cancelled_at timestamp
// ARGV[8..] = replacement hash field/value pairs
// Returns {outcome, existingID, existingState}.
var replaceUniqueJobScript = redis.NewScript(`
local uniqueKey = KEYS[1]
local newJobKey = KEYS[2]
local newSortedSetKey = KEYS[3]
local queuesKey = KEYS[4]
local scheduledKey = KEYS[5]
local retryKey = KEYS[6]

local expectedID = ARGV[1]
local relevantStates = ARGV[2]
local ttlMs = tonumber(ARGV[3])
local score = tonumber(ARGV[4])
local newJobID = ARGV[5]
local newQueue = ARGV[6]
local cancelledAt = ARGV[7]

local function keyType(key)
    return redis.call('TYPE', key).ok
end

local uniqueType = keyType(uniqueKey)
if uniqueType ~= 'none' and uniqueType ~= 'string' then
    return redis.error_reply('unique claim key must be a string')
end
if keyType(newJobKey) ~= 'none' then
    return redis.error_reply('unique replacement job already exists')
end
local newSortedType = keyType(newSortedSetKey)
if newSortedType ~= 'none' and newSortedType ~= 'zset' then
    return redis.error_reply('unique replacement sorted set key must be a sorted set')
end
local queuesType = keyType(queuesKey)
if queuesType ~= 'none' and queuesType ~= 'set' then
    return redis.error_reply('unique replacement queues key must be a set')
end
local scheduledType = keyType(scheduledKey)
if scheduledType ~= 'none' and scheduledType ~= 'zset' then
    return redis.error_reply('unique replacement scheduled key must be a sorted set')
end
local retryType = keyType(retryKey)
if retryType ~= 'none' and retryType ~= 'zset' then
    return redis.error_reply('unique replacement retry key must be a sorted set')
end
if ttlMs == nil or score == nil or newJobID == '' or newQueue == '' then
    return redis.error_reply('unique replacement arguments are invalid')
end

local actualID = redis.call('GET', uniqueKey) or ''
if actualID ~= expectedID then
    local actualState = ''
    if actualID ~= '' then
        local actualJobKey = 'ojs:job:' .. actualID
        local actualJobType = keyType(actualJobKey)
        if actualJobType ~= 'none' and actualJobType ~= 'hash' then
            return redis.error_reply('unique predecessor job key must be a hash')
        end
        if actualJobType == 'hash' then
            actualState = redis.call('HGET', actualJobKey, 'state') or ''
        end
    end
    return {'rejected', actualID, actualState}
end

local terminalStates = {completed = true, cancelled = true, discarded = true}
local function isRelevant(state)
    if state == false or state == nil or state == '' then
        return false
    end
    if relevantStates ~= '' then
        for item in string.gmatch(relevantStates, '([^,]+)') do
            if item == state then
                return true
            end
        end
        return false
    end
    return not terminalStates[state]
end

local replacedID = ''
if actualID ~= '' then
    local oldJobKey = 'ojs:job:' .. actualID
    local oldJobType = keyType(oldJobKey)
    if oldJobType ~= 'none' and oldJobType ~= 'hash' then
        return redis.error_reply('unique predecessor job key must be a hash')
    end
    if oldJobType == 'hash' then
        local oldState = redis.call('HGET', oldJobKey, 'state') or ''
        if isRelevant(oldState) then
            if oldState ~= 'available' and oldState ~= 'scheduled' and oldState ~= 'retryable' then
                return {'rejected', actualID, oldState}
            end

            local oldQueue = redis.call('HGET', oldJobKey, 'queue') or ''
            if oldQueue == '' then
                return redis.error_reply('unique predecessor queue is missing')
            end
            local oldAvailableKey = 'ojs:queue:' .. oldQueue .. ':available'
            local oldActiveKey = 'ojs:queue:' .. oldQueue .. ':active'
            local oldVisibilityKey = 'ojs:visibility:' .. actualID

            local oldAvailableType = keyType(oldAvailableKey)
            if oldAvailableType ~= 'none' and oldAvailableType ~= 'zset' then
                return redis.error_reply('unique predecessor available key must be a sorted set')
            end
            local oldActiveType = keyType(oldActiveKey)
            if oldActiveType ~= 'none' and oldActiveType ~= 'set' then
                return redis.error_reply('unique predecessor active key must be a set')
            end
            local oldVisibilityType = keyType(oldVisibilityKey)
            if oldVisibilityType ~= 'none' and oldVisibilityType ~= 'string' then
                return redis.error_reply('unique predecessor visibility key must be a string')
            end

            redis.call('HSET', oldJobKey,
                'state', 'cancelled',
                'cancelled_at', cancelledAt)
            redis.call('ZREM', oldAvailableKey, actualID)
            redis.call('SREM', oldActiveKey, actualID)
            redis.call('ZREM', scheduledKey, actualID)
            redis.call('ZREM', retryKey, actualID)
            redis.call('DEL', oldVisibilityKey)
            replacedID = actualID
        end
    end
end

local fields = {}
for i = 8, #ARGV do
    fields[#fields + 1] = ARGV[i]
end
if #fields > 0 then
    redis.call('HSET', newJobKey, unpack(fields))
end
redis.call('ZADD', newSortedSetKey, score, newJobID)
redis.call('SADD', queuesKey, newQueue)
if ttlMs > 0 then
    redis.call('SET', uniqueKey, newJobID, 'PX', ttlMs)
else
    redis.call('SET', uniqueKey, newJobID)
end
return {'claimed', replacedID, ''}
`)

// cancelWorkflowScript atomically fences a running workflow by transitioning it
// to cancelled, collecting every stable outbox job ID, deleting all pending or
// leased effects, and removing the workflow from the global pending index.
// Stable IDs are retained on the workflow hash so a retry can finish cancelling
// an effect job created just before the fence.
//
// KEYS[1] = workflow hash
// KEYS[2] = workflow effects hash
// KEYS[3] = set of workflow IDs with pending effects
// ARGV[1] = workflow ID
// ARGV[2] = completed_at timestamp
// Returns {appliedFlag, state, effectJobID...}.
var cancelWorkflowScript = redis.NewScript(`
local workflowKey = KEYS[1]
local effectsKey = KEYS[2]
local pendingKey = KEYS[3]
local workflowID = ARGV[1]
local completedAt = ARGV[2]

local function keyType(key)
    return redis.call('TYPE', key).ok
end

local workflowKeyType = keyType(workflowKey)
if workflowKeyType == 'none' then
    return {0, 'missing'}
end
if workflowKeyType ~= 'hash' then
    return redis.error_reply('workflow has an invalid type')
end
local effectsType = keyType(effectsKey)
if effectsType ~= 'none' and effectsType ~= 'hash' then
    return redis.error_reply('workflow effects key must be a hash')
end
local pendingType = keyType(pendingKey)
if pendingType ~= 'none' and pendingType ~= 'set' then
    return redis.error_reply('workflow pending index must be a set')
end

local state = redis.call('HGET', workflowKey, 'state') or ''
if state ~= 'running' and state ~= 'cancelled' then
    return {0, state}
end

local ids = {}
local seen = {}
local function addID(jobID)
    if jobID ~= nil and jobID ~= '' and not seen[jobID] then
        seen[jobID] = true
        ids[#ids + 1] = jobID
    end
end

local retained = redis.call('HGET', workflowKey, 'cancel_effect_job_ids') or ''
for jobID in string.gmatch(retained, '([^,]+)') do
    addID(jobID)
end

if effectsType == 'hash' then
    local values = redis.call('HVALS', effectsKey)
    for _, value in ipairs(values) do
        local parts = {}
        for field in string.gmatch(value, '([^|]+)') do
            parts[#parts + 1] = field
        end
        if parts[1] == 'active' then
            addID(parts[4])
        else
            addID(parts[2])
        end
    end
end

local applied = 0
if state == 'running' then
    applied = 1
    state = 'cancelled'
    redis.call('HSET', workflowKey,
        'state', state,
        'completed_at', completedAt)
end
if #ids > 0 then
    redis.call('HSET', workflowKey, 'cancel_effect_job_ids', table.concat(ids, ','))
end
redis.call('DEL', effectsKey)
redis.call('SREM', pendingKey, workflowID)

local result = {applied, state}
for _, jobID in ipairs(ids) do
    result[#result + 1] = jobID
end
return result
`)

// advanceWorkflowScript atomically records one job outcome, increments the
// workflow counters, grants terminal/chain ownership to exactly one caller, and
// durably records the required dispatch effects (the next chain step and any
// applicable batch callbacks) into a persistent outbox so a crash after this
// transition never loses the work and never requires the same worker to retry.
//
// KEYS[1] = workflow hash
// KEYS[2] = set of job IDs already applied
// KEYS[3] = workflow result hash
// KEYS[4] = workflow effects hash (the durable outbox)
// KEYS[5] = set of workflow IDs with pending effects
// ARGV[1]  = job ID
// ARGV[2]  = workflow step index
// ARGV[3]  = result JSON (may be empty)
// ARGV[4]  = "1" for failure, "0" for success
// ARGV[5]  = terminal timestamp
// ARGV[6]  = workflow ID
// ARGV[7]  = preassigned next chain-step job ID
// ARGV[8]  = preassigned on_complete callback job ID
// ARGV[9]  = preassigned on_success callback job ID
// ARGV[10] = preassigned on_failure callback job ID
var advanceWorkflowScript = redis.NewScript(`
local workflowKey = KEYS[1]
local advancedKey = KEYS[2]
local resultsKey = KEYS[3]
local effectsKey = KEYS[4]
local pendingKey = KEYS[5]
local jobID = ARGV[1]
local step = tonumber(ARGV[2])
local result = ARGV[3]
local failed = ARGV[4] == '1'
local completedAt = ARGV[5]
local workflowID = ARGV[6]
local nextChainJobID = ARGV[7]
local onCompleteJobID = ARGV[8]
local onSuccessJobID = ARGV[9]
local onFailureJobID = ARGV[10]

local function keyType(key)
    return redis.call('TYPE', key).ok
end

if keyType(workflowKey) ~= 'hash' then
    return redis.error_reply('workflow is missing or has an invalid type')
end
local advancedType = keyType(advancedKey)
if advancedType ~= 'none' and advancedType ~= 'set' then
    return redis.error_reply('workflow advancement key must be a set')
end
local resultsType = keyType(resultsKey)
if resultsType ~= 'none' and resultsType ~= 'hash' then
    return redis.error_reply('workflow results key must be a hash')
end
local effectsType = keyType(effectsKey)
if effectsType ~= 'none' and effectsType ~= 'hash' then
    return redis.error_reply('workflow effects key must be a hash')
end
local pendingType = keyType(pendingKey)
if pendingType ~= 'none' and pendingType ~= 'set' then
    return redis.error_reply('workflow pending index must be a set')
end
if step == nil or step < 0 then
    return redis.error_reply('workflow step must be a non-negative integer')
end

local function pendingEffectsFlag()
    if redis.call('HLEN', effectsKey) > 0 then
        return 1
    end
    return 0
end

local workflowType = redis.call('HGET', workflowKey, 'type') or ''
local state = redis.call('HGET', workflowKey, 'state') or ''
local total = tonumber(redis.call('HGET', workflowKey, 'total'))
local completed = tonumber(redis.call('HGET', workflowKey, 'completed'))
local failedCount = tonumber(redis.call('HGET', workflowKey, 'failed'))

if workflowType ~= 'chain' and workflowType ~= 'group' and workflowType ~= 'batch' then
    return redis.error_reply('workflow has an unsupported type')
end
if total == nil or total < 1 or completed == nil or completed < 0 or failedCount == nil or failedCount < 0 then
    return redis.error_reply('workflow counters are invalid')
end
if state ~= 'running' then
    return {0, workflowType, state, completed, failedCount, total, 0, 0, -1, pendingEffectsFlag()}
end
if redis.call('SADD', advancedKey, jobID) == 0 then
    return {0, workflowType, state, completed, failedCount, total, 0, 0, -1, pendingEffectsFlag()}
end

if result ~= '' then
    redis.call('HSET', resultsKey, tostring(step), result)
end
if failed then
    failedCount = failedCount + 1
else
    completed = completed + 1
end

local terminalOwner = 0
local enqueueNext = 0
local nextStep = -1
local finished = completed + failedCount

if workflowType == 'chain' then
    if failed then
        state = 'failed'
        terminalOwner = 1
    elseif finished >= total then
        state = 'completed'
        terminalOwner = 1
    else
        enqueueNext = 1
        nextStep = step + 1
    end
elseif finished >= total then
    if failedCount > 0 then
        state = 'failed'
    else
        state = 'completed'
    end
    terminalOwner = 1
end

redis.call('HSET', workflowKey,
    'completed', tostring(completed),
    'failed', tostring(failedCount),
    'state', state)
if terminalOwner == 1 then
    redis.call('HSET', workflowKey, 'completed_at', completedAt)
end

-- Durably record dispatch effects so the follow-up enqueues survive a crash.
local function recordEffect(effectID, effectJobID)
    if redis.call('HSETNX', effectsKey, effectID, 'pending|' .. effectJobID) == 1 then
        redis.call('SADD', pendingKey, workflowID)
    end
end

if enqueueNext == 1 then
    recordEffect('chain:' .. tostring(nextStep), nextChainJobID)
elseif terminalOwner == 1 and workflowType == 'batch' then
    local callbacks = redis.call('HGET', workflowKey, 'callbacks')
    if callbacks ~= false and callbacks ~= '' then
        recordEffect('callback:on_complete', onCompleteJobID)
        if failedCount > 0 then
            recordEffect('callback:on_failure', onFailureJobID)
        else
            recordEffect('callback:on_success', onSuccessJobID)
        end
    end
end

return {1, workflowType, state, completed, failedCount, total, terminalOwner, enqueueNext, nextStep, pendingEffectsFlag()}
`)

// claimWorkflowEffectScript leases one pending (or lease-expired) effect for a
// single drainer so concurrent drains cannot double-dispatch or reset a job.
// KEYS[1] = workflow hash
// KEYS[2] = workflow effects hash
// ARGV[1] = effect ID
// ARGV[2] = owner token
// ARGV[3] = current time in milliseconds
// ARGV[4] = lease duration in milliseconds
// Returns {status, jobID} where status is claimed|busy|done|gone|fenced.
var claimWorkflowEffectScript = redis.NewScript(`
local workflowKey = KEYS[1]
local effectsKey = KEYS[2]
local effectID = ARGV[1]
local owner = ARGV[2]
local nowMs = tonumber(ARGV[3])
local leaseMs = tonumber(ARGV[4])

local function keyType(key)
    return redis.call('TYPE', key).ok
end

if nowMs == nil or leaseMs == nil or leaseMs <= 0 then
    return redis.error_reply('workflow effect lease arguments are invalid')
end
if keyType(workflowKey) ~= 'hash' then
    return redis.error_reply('workflow is missing or has an invalid type')
end
if redis.call('HGET', workflowKey, 'state') == 'cancelled' then
    return {'fenced', ''}
end
local effectsType = keyType(effectsKey)
if effectsType == 'none' then
    return {'gone', ''}
end
if effectsType ~= 'hash' then
    return redis.error_reply('workflow effects key must be a hash')
end

local current = redis.call('HGET', effectsKey, effectID)
if current == false then
    return {'gone', ''}
end

local parts = {}
for field in string.gmatch(current, '([^|]+)') do
    parts[#parts + 1] = field
end
local status = parts[1]

if status == 'done' then
    return {'done', parts[2] or ''}
end
if status == 'pending' then
    local effectJobID = parts[2] or ''
    redis.call('HSET', effectsKey, effectID, 'active|' .. owner .. '|' .. tostring(nowMs + leaseMs) .. '|' .. effectJobID)
    return {'claimed', effectJobID}
end
if status == 'active' then
    local leaseUntil = tonumber(parts[3])
    local effectJobID = parts[4] or ''
    if leaseUntil ~= nil and leaseUntil > nowMs then
        return {'busy', effectJobID}
    end
    redis.call('HSET', effectsKey, effectID, 'active|' .. owner .. '|' .. tostring(nowMs + leaseMs) .. '|' .. effectJobID)
    return {'claimed', effectJobID}
end
return redis.error_reply('workflow effect has an invalid state')
`)

// createWorkflowEffectJobScript atomically verifies that the workflow is not
// cancelled and that the caller still owns the effect lease, then creates the
// stable effect job only if it is absent. Existing jobs are never rewritten or
// re-indexed, including fetched and terminal jobs.
//
// KEYS[1] = workflow hash
// KEYS[2] = workflow effects hash
// KEYS[3] = stable job hash
// KEYS[4] = queue available or scheduled sorted set
// KEYS[5] = queues set
// ARGV[1] = effect ID
// ARGV[2] = owner token
// ARGV[3] = score
// ARGV[4] = job ID
// ARGV[5] = queue name
// ARGV[6..] = job hash field/value pairs
// Returns created|existing|fenced|not_owner|gone.
var createWorkflowEffectJobScript = redis.NewScript(`
local workflowKey = KEYS[1]
local effectsKey = KEYS[2]
local jobKey = KEYS[3]
local sortedSetKey = KEYS[4]
local queuesKey = KEYS[5]
local effectID = ARGV[1]
local owner = ARGV[2]
local score = tonumber(ARGV[3])
local jobID = ARGV[4]
local queueName = ARGV[5]

local function keyType(key)
    return redis.call('TYPE', key).ok
end

if keyType(workflowKey) ~= 'hash' then
    return redis.error_reply('workflow is missing or has an invalid type')
end
if redis.call('HGET', workflowKey, 'state') == 'cancelled' then
    return 'fenced'
end

local effectsType = keyType(effectsKey)
if effectsType == 'none' then
    return 'gone'
end
if effectsType ~= 'hash' then
    return redis.error_reply('workflow effects key must be a hash')
end

local current = redis.call('HGET', effectsKey, effectID)
if current == false then
    return 'gone'
end
local parts = {}
for field in string.gmatch(current, '([^|]+)') do
    parts[#parts + 1] = field
end
if parts[1] ~= 'active' or parts[2] ~= owner then
    return 'not_owner'
end
if (parts[4] or '') ~= jobID then
    return redis.error_reply('workflow effect stable job ID mismatch')
end

if redis.call('EXISTS', jobKey) == 1 then
    return 'existing'
end

local sortedType = keyType(sortedSetKey)
if sortedType ~= 'none' and sortedType ~= 'zset' then
    return redis.error_reply('workflow effect sorted set key must be a sorted set')
end
local queuesType = keyType(queuesKey)
if queuesType ~= 'none' and queuesType ~= 'set' then
    return redis.error_reply('workflow effect queues key must be a set')
end
if score == nil or jobID == '' or queueName == '' then
    return redis.error_reply('workflow effect job arguments are invalid')
end

local fields = {}
for i = 6, #ARGV do
    fields[#fields + 1] = ARGV[i]
end
if #fields > 0 then
    redis.call('HSET', jobKey, unpack(fields))
end
redis.call('ZADD', sortedSetKey, score, jobID)
redis.call('SADD', queuesKey, queueName)
return 'created'
`)

// completeWorkflowEffectScript marks a leased effect done exactly once. It
// appends the job to the workflow job list only for chain effects, and only on
// the transition from active to done, so a retry can never duplicate the entry.
// When no effects remain pending the outbox is cleaned up.
// KEYS[1] = workflow hash
// KEYS[2] = workflow effects hash
// KEYS[3] = workflow job list
// KEYS[4] = set of workflow IDs with pending effects
// ARGV[1] = effect ID
// ARGV[2] = owner token
// ARGV[3] = job ID
// ARGV[4] = "1" to append the job to the workflow job list, else "0"
// ARGV[5] = workflow ID
var completeWorkflowEffectScript = redis.NewScript(`
local workflowKey = KEYS[1]
local effectsKey = KEYS[2]
local jobsKey = KEYS[3]
local pendingKey = KEYS[4]
local effectID = ARGV[1]
local owner = ARGV[2]
local jobID = ARGV[3]
local appendJob = ARGV[4] == '1'
local workflowID = ARGV[5]

local function keyType(key)
    return redis.call('TYPE', key).ok
end

if keyType(workflowKey) ~= 'hash' then
    return redis.error_reply('workflow is missing or has an invalid type')
end
local effectsType = keyType(effectsKey)
if effectsType ~= 'none' and effectsType ~= 'hash' then
    return redis.error_reply('workflow effects key must be a hash')
end
local jobsType = keyType(jobsKey)
if jobsType ~= 'none' and jobsType ~= 'list' then
    return redis.error_reply('workflow jobs key must be a list')
end
local pendingType = keyType(pendingKey)
if pendingType ~= 'none' and pendingType ~= 'set' then
    return redis.error_reply('workflow pending index must be a set')
end
if redis.call('HGET', workflowKey, 'state') == 'cancelled' then
    return 0
end
if effectsType == 'none' then
    return 0
end

local function cleanupIfDrained()
    local values = redis.call('HVALS', effectsKey)
    for _, value in ipairs(values) do
        if string.sub(value, 1, 5) ~= 'done|' then
            return
        end
    end
    redis.call('DEL', effectsKey)
    redis.call('SREM', pendingKey, workflowID)
end

local current = redis.call('HGET', effectsKey, effectID)
if current == false then
    cleanupIfDrained()
    return 0
end

local parts = {}
for field in string.gmatch(current, '([^|]+)') do
    parts[#parts + 1] = field
end
local status = parts[1]

if status == 'done' then
    cleanupIfDrained()
    return 0
end
if status ~= 'active' or parts[2] ~= owner then
    return 0
end

if appendJob then
    redis.call('RPUSH', jobsKey, jobID)
end
redis.call('HSET', effectsKey, effectID, 'done|' .. jobID)
cleanupIfDrained()
return 1
`)

// releaseWorkflowEffectScript returns a leased effect to pending after a failed
// dispatch so another drainer (or the same one) can retry promptly instead of
// waiting for the lease to expire.
// KEYS[1] = workflow effects hash
// ARGV[1] = effect ID
// ARGV[2] = owner token
var releaseWorkflowEffectScript = redis.NewScript(`
local effectsKey = KEYS[1]
local effectID = ARGV[1]
local owner = ARGV[2]

local effectsType = redis.call('TYPE', effectsKey).ok
if effectsType == 'none' then
    return 0
end
if effectsType ~= 'hash' then
    return redis.error_reply('workflow effects key must be a hash')
end

local current = redis.call('HGET', effectsKey, effectID)
if current == false then
    return 0
end
local parts = {}
for field in string.gmatch(current, '([^|]+)') do
    parts[#parts + 1] = field
end
if parts[1] ~= 'active' or parts[2] ~= owner then
    return 0
end
redis.call('HSET', effectsKey, effectID, 'pending|' .. (parts[4] or ''))
return 1
`)
