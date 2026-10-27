package main

// Row-key segment encoding, in one place.
//
// A row key is the primary-key columns joined by kvSep. Numbers and ids are
// wrapped as {"__fluxer_type":"bigint","value":"…"} (bigintSeg), but a text
// column is stored as a plain JSON string, quotes included: the instance_configuration
// rows read "account_identity_config" with the quotes. Getting that wrong does
// not raise an error — the row simply lands under a key nobody reads.

import "encoding/json"

// stringSeg encodes one text column for a row key.
func stringSeg(s string) string {
	raw, err := json.Marshal(s)
	if err != nil {
		// A Go string always marshals; the branch exists so the signature stays
		// honest instead of panicking in a worker loop.
		return `""`
	}
	return string(raw)
}

// boolSeg encodes one boolean column, which the api stores as true/false.
func boolSeg(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
