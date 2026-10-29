-- Minimal Mastra Code store: one thread with a user signal, an assistant turn
-- (reasoning, tool call with result, text) and harness scaffolding to skip.
CREATE TABLE mastra_threads (
    id TEXT PRIMARY KEY,
    resourceId TEXT NOT NULL,
    title TEXT,
    metadata TEXT,
    createdAt TEXT NOT NULL,
    updatedAt TEXT NOT NULL
);
CREATE TABLE mastra_messages (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    content TEXT NOT NULL,
    role TEXT NOT NULL,
    type TEXT,
    createdAt TEXT NOT NULL,
    resourceId TEXT
);
INSERT INTO mastra_threads VALUES
    ('thread_main', 'res', 'Listing files', '{"projectPath":"/work/proj"}',
     '2026-10-20T04:00:00.000Z', '2026-10-20T04:00:05.000Z'),
    ('thread_empty', 'res', 'No conversation', '{"projectPath":"/work/other"}',
     '2026-10-20T04:10:00.000Z', '2026-10-20T04:10:00.000Z');
INSERT INTO mastra_messages VALUES
    ('m1', 'thread_main',
     '{"format":2,"parts":[{"type":"text","text":"List the files"}]}',
     'signal', 'user', '2026-10-20T04:00:01.000Z', 'res'),
    ('m2', 'thread_main',
     '{"format":2,"parts":[{"type":"step-start"},{"type":"reasoning","reasoning":"thinking about it","details":[]},{"type":"tool-invocation","toolInvocation":{"state":"result","toolCallId":"call_1","toolName":"shell","args":{"command":"ls"},"result":"a.txt"}},{"type":"data-om-buffering-start"},{"type":"text","text":"One file"}]}',
     'assistant', 'v2', '2026-10-20T04:00:02.000Z', 'res'),
    ('m3', 'thread_main',
     '{"format":2,"parts":[{"type":"text","text":"<system-reminder>ignore</system-reminder>"}]}',
     'signal', 'system-reminder', '2026-10-20T04:00:03.000Z', 'res'),
    ('m4', 'thread_empty',
     '{"format":2,"parts":[{"type":"text","text":"<system-reminder>only scaffolding</system-reminder>"}]}',
     'signal', 'system-reminder', '2026-10-20T04:10:01.000Z', 'res');
