package main

// The rest of the search indexing lane: guild members, the users ACL backfill,
// the discovery index, and the admin reindex. They share searchWriterFor() and
// the index specs in searchdocs.go with the tasks in searchindex.go.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	guildMembersPageSize    = 1000
	guildMembersIndexChunk  = 500
	userACLBackfillDoneKey  = "backfill:user_acl_index:done"
	userACLBackfillClaimKey = "backfill:user_acl_index:queued"
	userACLBackfillClaimTTL = 6 * 60 * 60
	userACLBackfillPageSize = 500
	minUsernameSuffixLength = 2
	discoveryGuildsPageSize = 500
)

// taskIndexGuildMembers ports IndexGuildMembers.ts: page a guild's members, join
// each with its user row, and index the pair. Members whose user row is gone are
// skipped, which is what upstream's flatMap does.
func taskIndexGuildMembers(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return err
		}
	}
	guildID := stringField(payload, "guildId")
	if !isDigits(guildID) {
		return errors.New("indexGuildMembers: guildId is required")
	}
	lastUserID := stringField(payload, "lastUserId")
	if lastUserID != "" && !isDigits(lastUserID) {
		return errors.New("indexGuildMembers: lastUserId is not a snowflake")
	}

	writer := searchWriterFor()
	cursor := lastUserID
	totalIndexed := 0
	for {
		members, err := readGuildMemberPage(ctx, guildID, cursor, guildMembersPageSize)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			break
		}
		userIDs := make([]string, 0, len(members))
		seen := map[string]bool{}
		for _, m := range members {
			id := typedString(m["user_id"])
			if id != "" && !seen[id] {
				seen[id] = true
				userIDs = append(userIDs, id)
			}
		}
		users, err := readUsersByID(ctx, userIDs)
		if err != nil {
			return err
		}
		docs := make([]map[string]any, 0, len(members))
		for _, m := range members {
			userID := typedString(m["user_id"])
			user, ok := users[userID]
			if !ok {
				continue
			}
			docs = append(docs, guildMemberSearchDoc(guildID, m, user))
		}
		for start := 0; start < len(docs); start += guildMembersIndexChunk {
			end := start + guildMembersIndexChunk
			if end > len(docs) {
				end = len(docs)
			}
			if _, err := writer.upsert(ctx, searchIndexGuildMembers, docs[start:end]); err != nil {
				return err
			}
		}
		totalIndexed += len(docs)
		if len(members) < guildMembersPageSize {
			break
		}
		cursor = typedString(members[len(members)-1]["user_id"])
	}

	if err := patchGuildMembersIndexedAt(ctx, guildID); err != nil {
		log.Printf("indexGuildMembers: %s: patch members_indexed_at: %v", guildID, err)
	}
	log.Printf("indexGuildMembers: guild=%s indexed=%d", guildID, totalIndexed)
	return nil
}

func readGuildMemberPage(ctx context.Context, guildID, afterUserID string, limit int) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data
FROM fluxer_kv
WHERE table_name = 'guild_members'
  AND row_data->'guild_id'->>'value' = $1
  AND ($2 = '' OR row_data->'user_id'->>'value' > $2)
ORDER BY row_data->'user_id'->>'value'
LIMIT $3`, guildID, afterUserID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		data := map[string]any{}
		if err := json.Unmarshal(raw, &data); err != nil {
			continue
		}
		out = append(out, data)
	}
	return out, rows.Err()
}

// readUsersByID fetches the user rows for one member page. It is a single ANY()
// query rather than a loop: a 1000-member page would otherwise be 1000 round
// trips to the database that lives on another host.
func readUsersByID(ctx context.Context, userIDs []string) (map[string]map[string]any, error) {
	out := make(map[string]map[string]any, len(userIDs))
	if len(userIDs) == 0 {
		return out, nil
	}
	rows, err := db.QueryContext(ctx, `
SELECT row_data
FROM fluxer_kv
WHERE table_name = 'users' AND row_data->'user_id'->>'value' = ANY($1)`, userIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		data := map[string]any{}
		if err := json.Unmarshal(raw, &data); err != nil {
			continue
		}
		if id := typedString(data["user_id"]); id != "" {
			out[id] = data
		}
	}
	return out, rows.Err()
}

// guildMemberSearchDoc ports convertToSearchableGuildMember. The id is the pair
// joined with an underscore, which is what upstream indexes and what its own
// delete path addresses.
func guildMemberSearchDoc(guildID string, member, user map[string]any) map[string]any {
	userID := typedString(member["user_id"])
	username := typedString(user["username"])
	joinedAt := int64(0)
	if ts := dateSeconds(member["joined_at"]); ts != nil {
		joinedAt = *ts
	}
	return map[string]any{
		"id":               guildID + "_" + userID,
		"guildId":          guildID,
		"userId":           userID,
		"username":         username,
		"usernameSearch":   buildUsernameSearch(username),
		"discriminator":    discriminatorString(intOrZero(user["discriminator"])),
		"globalName":       nullIfEmpty(typedString(user["global_name"])),
		"nickname":         nullIfEmpty(typedString(member["nick"])),
		"roleIds":          typedStringSlice(member["role_ids"]),
		"joinedAt":         joinedAt,
		"joinSourceType":   intOrZero(member["join_source_type"]),
		"sourceInviteCode": nullIfEmpty(typedString(member["source_invite_code"])),
		"inviterId":        nullIfEmpty(typedString(member["inviter_id"])),
		"userCreatedAt":    snowflakeSeconds(userID),
		"isBot":            boolField(user, "bot"),
	}
}

// buildUsernameSearch is the suffix list upstream builds: every tail of the
// lowercased username at least two characters long, so "doesnm" matches "esnm".
func buildUsernameSearch(username string) string {
	normalized := strings.ToLower(strings.TrimSpace(username))
	if len(normalized) < minUsernameSuffixLength {
		return normalized
	}
	seen := make(map[string]struct{}, len(normalized))
	tokens := make([]string, 0, len(normalized))
	for i := 0; i+minUsernameSuffixLength <= len(normalized); i++ {
		token := normalized[i:]
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		tokens = append(tokens, token)
	}
	return strings.Join(tokens, " ")
}

func discriminatorString(d int) string {
	s := fmt.Sprintf("%d", d)
	if len(s) >= 4 {
		return s
	}
	return strings.Repeat("0", 4-len(s)) + s
}

func patchGuildMembersIndexedAt(ctx context.Context, guildID string) error {
	_, err := db.ExecContext(ctx, `
UPDATE fluxer_kv
SET row_data = row_data || jsonb_build_object('members_indexed_at', $2::jsonb), updated_at = now()
WHERE table_name = 'guilds' AND row_key = $1`, bigintSeg(guildID), string(mustJSON(kvDate(time.Now().UTC()))))
	return err
}
