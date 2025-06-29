SELECT 
    h.hacker_id,
    h.name
FROM Hackers h
JOIN (
    SELECT 
        s.hacker_id,
        COUNT(DISTINCT s.challenge_id) AS full_score_count
    FROM Submissions s
    JOIN Challenges c ON s.challenge_id = c.challenge_id
    JOIN Difficulty d ON c.difficulty_level = d.difficulty_level
    WHERE s.score = d.score
    GROUP BY s.hacker_id
    HAVING COUNT(DISTINCT s.challenge_id) > 1
) AS fs ON h.hacker_id = fs.hacker_id
ORDER BY fs.full_score_count DESC, h.hacker_id ASC;