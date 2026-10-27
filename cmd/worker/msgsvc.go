package main

// Message and user responses come from the api's own services over NATS.
//
// This is the discovery that changes the shape of the remaining work: the api
// subscribes to `svc.messages` (ops ListResponses, ExtractMentions,
// GetResponseById, BuildResponse, BuildResponses) and to `svc.users`, and its own
// worker uses them instead of mapping a message in the worker process. Porting a
// message response mapper here — embeds, attachments, mentions, reactions, thread
// masks, permission-gated fields — would be a second implementation of the same
// thing, and the first place it drifts is the place where clients see it.
//
// So the worker asks, exactly as the api's worker does. The payload is the api's:
// the op, the ids, a viewer, and an access envelope (guild, history cutoff,
// whether the viewer may read history, hidden included, plus the media endpoint and
// the attachment-URL secret, because the response carries signed attachment URLs).
//
// When there is no connection (tests, a worker started without NATS) the call fails
// loudly: a caller that cannot get a response must decide what to do, not receive an
// empty object that looks like a real message.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const (
	messagesServiceSubject = "svc.messages"
	usersServiceSubject    = "svc.users"
	messageServiceTimeout  = 6 * time.Second
)

// messageResponseAccess is the api's MessageResponseAccessContext.
type messageResponseAccess struct {
	SourceGuildID         string
	MessageHistoryCutoff  *int64
	CanReadMessageHistory bool
	IncludeHidden         bool
}

// messageResponsePayload builds the GetResponseById request body.
func messageResponsePayload(viewerUserID, channelID, messageID string, access messageResponseAccess) map[string]any {
	var cutoff any
	if access.MessageHistoryCutoff != nil {
		cutoff = *access.MessageHistoryCutoff
	}
	return map[string]any{
		"op":                           "GetResponseById",
		"channel_id":                   channelID,
		"message_id":                   messageID,
		"viewer_user_id":               viewerUserID,
		"source_guild_id":              nullableString(access.SourceGuildID),
		"message_history_cutoff_ms":    cutoff,
		"can_read_message_history":     access.CanReadMessageHistory,
		"include_hidden":               access.IncludeHidden,
		"media_endpoint":               env("FLUXER_MEDIA_ENDPOINT", ""),
		"media_proxy_secret_key":       os.Getenv("FLUXER_MEDIA_PROXY_SECRET_KEY"),
		"attachment_url_secret_base64": os.Getenv("FLUXER_ATTACHMENT_URL_SECRET"),
		"instance_name":                env("FLUXER_INSTANCE_NAME", ""),
		"include_reactions":            true,
	}
}

// getMessageResponse asks the api for one message response. A nil result means the
// api answered NotFound.
func getMessageResponse(ctx context.Context, viewerUserID, channelID, messageID string, access messageResponseAccess) (map[string]any, error) {
	payload := messageResponsePayload(viewerUserID, channelID, messageID, access)
	// The api includes the decay flag only when it is on, which is why it is a
	// request-time value rather than part of the static envelope.
	// The decay flag is read from the database, so a caller without one (a unit
	// test, or a worker that has not opened the database yet) simply omits it: the
	// api treats an absent flag as "decay off" for URL signing.
	if db != nil {
		if enabled, err := attachmentDecayEnabled(ctx); err == nil && enabled {
			payload["attachment_decay"] = true
		}
	}
	raw, err := svcRequest(ctx, messagesServiceSubject, payload)
	if err != nil {
		return nil, err
	}
	var resp struct {
		FoundApi map[string]any `json:"FoundApi"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("svc.messages: decode: %w", err)
	}
	if resp.FoundApi == nil {
		// "NotFound" is a bare string in the contract; anything else is worth a
		// complaint, because a silently empty response would look like a message.
		var notFound string
		if json.Unmarshal(raw, &notFound) == nil && notFound == "NotFound" {
			return nil, nil
		}
		return nil, fmt.Errorf("svc.messages: unexpected response %s", truncateString(string(raw), 120))
	}
	return resp.FoundApi, nil
}

// truncateString keeps a diagnostic body readable in a log line.
func truncateString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// svcRequest is a plain NATS request with a JSON body.
func svcRequest(ctx context.Context, subject string, payload map[string]any) ([]byte, error) {
	nc := workerNATS()
	if nc == nil {
		return nil, fmt.Errorf("no NATS connection")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	msg, err := nc.RequestWithContext(ctx, subject, body)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", subject, err)
	}
	return msg.Data, nil
}
