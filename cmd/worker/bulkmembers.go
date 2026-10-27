package main

// taskBulkAddGuildMembers ports admin_bulk/BulkAddGuildMembers.ts.
//
// The admin panel can push a list of users into a guild. Per user the worker
// writes the two member rows (guild_members and the by-user index), bumps the
// guild's member_count, tells the guild's clients with GUILD_MEMBER_ADD, and
// records one admin audit entry; finally one summary entry for the sweep.
//
// Not ported, both because this worker has no such machinery: the join message
// into the guild's system channel, and the per-user welcome DM. Upstream's
// forceAdd also runs ban and account-limit checks it explicitly skips, so it is
// the skip path that is ported here — which is exactly what the admin action
// means.
//
// A user who must not be rendered (hidden profile or under enforcement, see
// userresponse.go) is skipped and counted: a member row without the event would
// leave clients without the member, and an event with the real profile would
// leak it.

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
)

const joinSourceAdminForceAdd = 4

func taskBulkAddGuildMembers(ctx context.Context, payload map[string]any) error {
	if db == nil {
		if err := openDB(); err != nil {
			return fmt.Errorf("bulkAddGuildMembers: %w", err)
		}
	}
	adminUserID := stringField(payload, "admin_user_id")
	reason := stringField(payload, "audit_log_reason")
	guildID := stringField(payload, "guild_id")
	userIDs := stringSliceField(payload, "user_ids")
	if adminUserID == "" || !isDigits(adminUserID) {
		return fmt.Errorf("bulkAddGuildMembers: admin_user_id is required")
	}
	if !isDigits(guildID) {
		return fmt.Errorf("bulkAddGuildMembers: guild_id is required")
	}
	if len(userIDs) == 0 {
		return fmt.Errorf("bulkAddGuildMembers: user_ids is empty")
	}
	guild, err := readKV(ctx, guildsTable, bigintSeg(guildID))
	if err != nil {
		return fmt.Errorf("bulkAddGuildMembers: read guild: %w", err)
	}
	if guild == nil {
		return fmt.Errorf("bulkAddGuildMembers: guild %s does not exist", guildID)
	}

	added, alreadyMembers, hidden, failed, err := applyGuildMemberAdds(ctx, adminUserID, reason, guildID, userIDs)
	if err != nil {
		return fmt.Errorf("bulkAddGuildMembers: %w", err)
	}

	metadata := [][2]string{
		{"guild_id", guildID},
		{"user_count", strconv.Itoa(len(userIDs))},
		{"added", strconv.Itoa(added)},
	}
	if alreadyMembers > 0 {
		metadata = append(metadata, [2]string{"already_members", strconv.Itoa(alreadyMembers)})
	}
	if hidden > 0 {
		metadata = append(metadata, [2]string{"hidden", strconv.Itoa(hidden)})
	}
	if failed > 0 {
		metadata = append(metadata, [2]string{"failed", strconv.Itoa(failed)})
	}
	if err := writeAdminAuditLog(ctx, adminUserID, "guild", guildID, "bulk_add_guild_members", reason, metadata); err != nil {
		return fmt.Errorf("bulkAddGuildMembers: summary audit: %w", err)
	}
	log.Printf("bulkAddGuildMembers: guild=%s users=%d added=%d already=%d hidden=%d failed=%d",
		guildID, len(userIDs), added, alreadyMembers, hidden, failed)
	return nil
}

// applyGuildMemberAdds does the per-user work and returns the counters the
// summary needs. Split out so the database half is testable without NATS.
func applyGuildMemberAdds(ctx context.Context, adminUserID, reason, guildID string, userIDs []string) (added, alreadyMembers, hidden, failed int, err error) {
	for _, userID := range userIDs {
		if !isDigits(userID) {
			failed++
			log.Printf("bulkAddGuildMembers: skipping malformed user id %q", userID)
			continue
		}
		memberKey := bigintSeg(guildID) + kvSep + bigintSeg(userID)
		existing, err := readKV(ctx, "guild_members", memberKey)
		if err != nil {
			failed++
			log.Printf("bulkAddGuildMembers: %s: read member: %v", userID, err)
			continue
		}
		if existing != nil {
			alreadyMembers++
			continue
		}
		user, err := readKV(ctx, "users", bigintSeg(userID))
		if err != nil {
			failed++
			log.Printf("bulkAddGuildMembers: %s: read user: %v", userID, err)
			continue
		}
		if user == nil {
			failed++
			log.Printf("bulkAddGuildMembers: user %s does not exist", userID)
			continue
		}
		now := time.Now().UTC()
		member := map[string]any{
			"guild_id":                     bigintValue(guildID),
			"user_id":                      bigintValue(userID),
			"joined_at":                    kvDate(now),
			"nick":                         nil,
			"avatar_hash":                  nil,
			"banner_hash":                  nil,
			"accent_color":                 nil,
			"role_ids":                     nil,
			"mute":                         false,
			"deaf":                         false,
			"communication_disabled_until": nil,
			"mention_flags":                nil,
			"profile_flags":                nil,
			"temporary":                    false,
			"inviter_id":                   nil,
			"join_source_type":             joinSourceAdminForceAdd,
			"version":                      1,
		}
		response, ok := guildMemberResponse(member, user, now)
		if !ok {
			hidden++
			log.Printf("bulkAddGuildMembers: user %s must not be rendered (hidden profile or enforcement), skipped", userID)
			continue
		}
		if err := putKV(ctx, "guild_members", memberKey, memberKey, member, ""); err != nil {
			failed++
			log.Printf("bulkAddGuildMembers: %s: write member: %v", userID, err)
			continue
		}
		byUserKey := bigintSeg(userID) + kvSep + bigintSeg(guildID)
		if err := putKV(ctx, "guild_members_by_user_id", byUserKey, byUserKey, map[string]any{
			"guild_id": bigintValue(guildID),
			"user_id":  bigintValue(userID),
		}, ""); err != nil {
			failed++
			log.Printf("bulkAddGuildMembers: %s: write index: %v", userID, err)
			continue
		}
		added++
		if nc := workerNATS(); nc != nil {
			data := map[string]any{"guild_id": guildID}
			for k, v := range response {
				data[k] = v
			}
			if err := dispatchGuild(nc, guildID, "GUILD_MEMBER_ADD", data); err != nil {
				log.Printf("bulkAddGuildMembers: %s: dispatch: %v (member added)", userID, err)
			}
		}
		if err := writeAdminAuditLog(ctx, adminUserID, "user", userID, "force_add_to_guild", reason,
			[][2]string{{"guild_id", guildID}}); err != nil {
			log.Printf("bulkAddGuildMembers: %s: audit: %v (member added)", userID, err)
		}
	}

	if added > 0 {
		if err := bumpGuildMemberCount(ctx, guildID, added); err != nil {
			log.Printf("bulkAddGuildMembers: member_count: %v (members are written)", err)
		}
	}
	return added, alreadyMembers, hidden, failed, nil
}

// bumpGuildMemberCount increments the denormalised counter the guild response
// carries. It is read-modify-write because the row stores the counter as a plain
// JSON number and there is no atomic increment over that representation.
func bumpGuildMemberCount(ctx context.Context, guildID string, delta int) error {
	row, err := readKV(ctx, guildsTable, bigintSeg(guildID))
	if err != nil || row == nil {
		return err
	}
	current, _ := intFieldChecked(map[string]any{"v": row["member_count"]}, "v")
	return patchRowField(ctx, guildsTable, bigintSeg(guildID), "member_count", current+delta)
}
