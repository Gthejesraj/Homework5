WITH session_data AS ( 
    SELECT sessionId, userId, channel 
    FROM {{ source('analytics', 'session_timestamp') }} 
    WHERE sessionId IS NOT NULL 
) 
SELECT * FROM session_data


