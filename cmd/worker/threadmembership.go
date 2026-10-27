package main

// taskRemoveThreadMembershipsForGuildMember ports
// RemoveThreadMembershipsForGuildMember.ts.
//
// When a person leaves a guild, their thread memberships have to go with them:
// each thread keeps a row in `thread_members` and a reverse row in
// `thread_members_by_user` that lets the guild list "threads I am in". Leaving the
// rows behind is how a former member keeps showing up inside threads of a guild
// they are no longer in.
//
// The api guards this task with the guild-taint marker (a guild whose rows may be
// mid-migration) and re-checks that the member is really gone before touching
// anything. Both are ported in spirit: the member check is here, and the taint
// marker is not — this instance never sets it.
//
// Not ported: the THREAD_MEMBERS_UPDATE dispatch (it needs a thread view mapper).
// The rows are correct immediately; clients see it on their next fetch.

import (
	"context"
	"fmt"
	"log"
	"strings"
)

func taskRemoveThreadMembershipsForGuildMember(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("removeThreadMembershipsForGuildMember: %w", err)
		}
	}
	guildID := stringField(payload, "guildId")
	userID := stringField(payload, "userId")
	if !isDigits(guildID) || !isDigits(userID) {
		return fmt.Errorf("removeThreadMembershipsForGuildMember: guildId and userId are required")
	}
	member, err := readKV(ctx, "guild_members", bigintSeg(guildID)+kvSep+bigintSeg(userID))
	if err != nil {
		return fmt.Errorf("removeThreadMembershipsForGuildMember: member check: %w", err)
	}
	if member != nil {
		// Still a member: the task only runs after the member row is gone.
		log.Printf("removeThreadMembershipsForGuildMember: user %s is still in guild %s, nothing to do", userID, guildID)
		return nil
	}

	threadIDs, err := joinedThreadsOfGuild(ctx, userID, guildID)
	if err != nil {
		return fmt.Errorf("removeThreadMembershipsForGuildMember: %w", err)
	}
	removed := 0
	for _, threadID := range threadIDs {
		res, err := db.ExecContext(ctx, `DELETE FROM fluxer_kv WHERE table_name = 'thread_members' AND row_key = $1`,
			bigintSeg(threadID)+kvSep+bigintSeg(userID))
		if err != nil {
			return fmt.Errorf("remove thread_members %s: %w", threadID, err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			removed++
		}
		if _, err := db.ExecContext(ctx, `
DELETE FROM fluxer_kv
WHERE table_name = 'thread_members_by_user'
  AND left(row_key, length($1)) = $1
  AND right(row_key, length($2)) = $2`,
			bigintSeg(userID)+kvSep, kvSep+bigintSeg(threadID)); err != nil {
			return fmt.Errorf("remove thread_members_by_user %s: %w", threadID, err)
		}
	}
	log.Printf("removeThreadMembershipsForGuildMember: user=%s guild=%s threads=%d removed=%d",
		userID, guildID, len(threadIDs), removed)
	return nil
}

// joinedThreadsOfGuild reads the thread ids out of thread_members_by_user, whose
// row key is "<user>\x1f<guild>\x1f<parent>\x1f<is_private>\x1f<thread>".
func joinedThreadsOfGuild(ctx context.Context, userID, guildID string) ([]string, error) {
	prefix := bigintSeg(userID) + kvSep + bigintSeg(guildID) + kvSep
	rows, err := db.QueryContext(ctx, `
SELECT row_key FROM fluxer_kv
WHERE table_name = 'thread_members_by_user' AND left(row_key, length($1)) = $1`, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var rowKey string
		if err := rows.Scan(&rowKey); err != nil {
			return nil, err
		}
		parts := strings.Split(rowKey, kvSep)
		if len(parts) < 5 {
			continue
		}
		if threadID := parseBigintSegment(parts[len(parts)-1]); threadID != "" {
			out = append(out, threadID)
		}
	}
	return out, rows.Err()
}

// parseBigintSegment reads the value out of one encoded bigint segment.
func parseBigintSegment(seg string) string {
	const prefix = `{"__fluxer_type":"bigint","value":"`
	if !strings.HasPrefix(seg, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(seg, prefix)
	idx := strings.Index(rest, `"`)
	if idx <= 0 {
		return ""
	}
	id := rest[:idx]
	if !isDigits(id) {
		return ""
	}
	return id
}
