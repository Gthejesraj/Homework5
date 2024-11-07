SELECT
    sessionId,
    userId,
    start_time,
    end_time
FROM {{ source('analytics', 'user_session_channel') }}
WHERE sessionId IS NOT NULL

