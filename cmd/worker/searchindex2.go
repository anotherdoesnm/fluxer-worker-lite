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

// --- users (ACL backfill) --------------------------------------------------

// taskBackfillUserACLIndex ports BackfillUserAclIndex.ts: walk every account once,
// index the ones that carry ACLs, then record that the backfill ran.
func taskBackfillUserACLIndex(ctx context.Context, _ map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return err
		}
	}
	writer := searchWriterFor()
	cursor := ""
	indexed := 0
	for {
		users, err := readUserPage(ctx, cursor, userACLBackfillPageSize)
		if err != nil {
			return err
		}
		if len(users) == 0 {
			break
		}
		docs := make([]map[string]any, 0, len(users))
		for _, user := range users {
			if len(typedStringSlice(user["acls"])) == 0 {
				continue
			}
			docs = append(docs, userSearchDoc(user))
		}
		if len(docs) > 0 {
			if _, err := writer.upsert(ctx, searchIndexUsers, docs); err != nil {
				return err
			}
			indexed += len(docs)
		}
		cursor = typedString(users[len(users)-1]["user_id"])
		if len(users) < userACLBackfillPageSize {
			break
		}
	}
	if valkey() != nil {
		if err := valkey().Set(ctx, userACLBackfillDoneKey, time.Now().UTC().Format(time.RFC3339), 0).Err(); err != nil {
			log.Printf("backfillUserAclIndex: marking done: %v", err)
		}
	}
	log.Printf("backfillUserAclIndex: indexed=%d", indexed)
	return nil
}

// readUserPage pages accounts by user id, which is the row's own key.
func readUserPage(ctx context.Context, afterUserID string, limit int) ([]map[string]any, error) {
	rows, err := db.QueryContext(ctx, `
SELECT row_data
FROM fluxer_kv
WHERE table_name = 'users' AND row_data->'user_id'->>'value' > $1
ORDER BY row_data->'user_id'->>'value'
LIMIT $2`, afterUserID, limit)
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

// userSearchDoc ports convertToSearchableUser for the fields this instance's user
// rows actually carry. It is the admin panel's user index: name, discriminator,
// flags, the boolean filters and the ACL set.
//
// Fields the upstream serializer also emits but that are absent here stay null:
// email (this instance signs in by username, so account rows carry no address),
// premiumType, tempBannedUntil, pendingDeletionAt and the two Stripe ids. A
// Meilisearch deployment would index them; nothing is lost for the filters that
// can actually match on this instance, and the nulls document the gap.
func userSearchDoc(user map[string]any) map[string]any {
	id := typedString(user["user_id"])
	var lastActiveAt any
	if ts := dateSeconds(user["last_active_at"]); ts != nil {
		lastActiveAt = *ts
	}
	flags := typedString(user["flags"])
	if flags == "" {
		flags = "0"
	}
	return map[string]any{
		"id":                id,
		"username":          typedString(user["username"]),
		"discriminator":     intOrZero(user["discriminator"]),
		"email":             nil,
		"isBot":             boolField(user, "bot"),
		"isSystem":          boolField(user, "system"),
		"flags":             flags,
		"premiumType":       nil,
		"emailVerified":     boolField(user, "email_verified"),
		"emailBounced":      boolField(user, "email_bounced"),
		"acls":              typedStringSlice(user["acls"]),
		"createdAt":         snowflakeSeconds(id),
		"lastActiveAt":      lastActiveAt,
		"tempBannedUntil":   nil,
		"pendingDeletionAt": nil,
	}
}
