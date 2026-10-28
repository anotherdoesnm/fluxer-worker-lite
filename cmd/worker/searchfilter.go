package main

// The Meilisearch filter dialect, compiled to SQL over the `doc` jsonb column.
//
// The API builds these strings itself (MeilisearchFilterUtils in the release the
// worker is pinned to), so the grammar is small and predictable:
//
//	field = value            field != value
//	field > value            field >= value
//	field < value            field <= value
//	field EXISTS             field NOT EXISTS
//	field IS NULL            field IS NOT NULL
//	(a AND b)                (a OR b)                NOT a
//
// Anything outside that returns an error: silently ignoring a filter would make
// a search return documents the caller asked to exclude, which is worse than a
// failed request.

import (
	"fmt"
	"strconv"
	"strings"
)

type filterTokenKind int

const (
	tokEOF filterTokenKind = iota
	tokWord
	tokString
	tokOpen
	tokClose
	tokOp
)

type filterToken struct {
	kind filterTokenKind
	text string
}

func tokenizeFilter(s string) ([]filterToken, error) {
	var out []filterToken
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(':
			out = append(out, filterToken{kind: tokOpen, text: "("})
			i++
		case c == ')':
			out = append(out, filterToken{kind: tokClose, text: ")"})
			i++
		case c == '"' || c == '\'':
			quote := c
			i++
			var sb strings.Builder
			closed := false
			for i < len(s) {
				if s[i] == '\\' && i+1 < len(s) {
					sb.WriteByte(s[i+1])
					i += 2
					continue
				}
				if s[i] == quote {
					closed = true
					i++
					break
				}
				sb.WriteByte(s[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated string in filter")
			}
			out = append(out, filterToken{kind: tokString, text: sb.String()})
		case c == '=' || c == '!' || c == '<' || c == '>':
			op := string(c)
			i++
			if i < len(s) && (s[i] == '=' || (c == '!' && s[i] == '=')) {
				op += string(s[i])
				i++
			}
			if op == "!" {
				return nil, fmt.Errorf("unexpected %q in filter", op)
			}
			out = append(out, filterToken{kind: tokOp, text: op})
		default:
			start := i
			for i < len(s) {
				ch := s[i]
				if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '(' || ch == ')' ||
					ch == '=' || ch == '!' || ch == '<' || ch == '>' {
					break
				}
				i++
			}
			out = append(out, filterToken{kind: tokWord, text: s[start:i]})
		}
	}
	out = append(out, filterToken{kind: tokEOF})
	return out, nil
}

type filterParser struct {
	tokens []filterToken
	pos    int
	argN   int
	args   []any
}

// compileSearchFilter turns a filter expression into a SQL fragment. argN is the
// number of the next positional parameter, so the caller can put its own
// arguments in front of the ones appended here.
func compileSearchFilter(filter string, argN int) (string, []any, error) {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return "", nil, nil
	}
	tokens, err := tokenizeFilter(filter)
	if err != nil {
		return "", nil, err
	}
	p := &filterParser{tokens: tokens, argN: argN}
	expr, err := p.parseOr()
	if err != nil {
		return "", nil, err
	}
	if p.peek().kind != tokEOF {
		return "", nil, fmt.Errorf("unexpected %q after filter expression", p.peek().text)
	}
	return expr, p.args, nil
}

func (p *filterParser) peek() filterToken { return p.tokens[p.pos] }

func (p *filterParser) next() filterToken {
	t := p.tokens[p.pos]
	if p.pos < len(p.tokens)-1 {
		p.pos++
	}
	return t
}

func (p *filterParser) acceptWord(word string) bool {
	if p.peek().kind == tokWord && strings.EqualFold(p.peek().text, word) {
		p.next()
		return true
	}
	return false
}

func (p *filterParser) parseOr() (string, error) {
	left, err := p.parseAnd()
	if err != nil {
		return "", err
	}
	for p.acceptWord("OR") {
		right, err := p.parseAnd()
		if err != nil {
			return "", err
		}
		left = "(" + left + " OR " + right + ")"
	}
	return left, nil
}

func (p *filterParser) parseAnd() (string, error) {
	left, err := p.parseUnary()
	if err != nil {
		return "", err
	}
	for p.acceptWord("AND") {
		right, err := p.parseUnary()
		if err != nil {
			return "", err
		}
		left = "(" + left + " AND " + right + ")"
	}
	return left, nil
}

func (p *filterParser) parseUnary() (string, error) {
	if p.acceptWord("NOT") {
		inner, err := p.parseUnary()
		if err != nil {
			return "", err
		}
		return "(NOT " + inner + ")", nil
	}
	if p.peek().kind == tokOpen {
		p.next()
		inner, err := p.parseOr()
		if err != nil {
			return "", err
		}
		if p.peek().kind != tokClose {
			return "", fmt.Errorf("missing closing parenthesis in filter")
		}
		p.next()
		return inner, nil
	}
	return p.parseTerm()
}

func (p *filterParser) parseTerm() (string, error) {
	field := p.next()
	if field.kind != tokWord || !attributeNameRE.MatchString(field.text) {
		return "", fmt.Errorf("expected a field name in filter, got %q", field.text)
	}
	col := "(doc->>'" + field.text + "')"

	// field EXISTS / field NOT EXISTS
	if p.acceptWord("EXISTS") {
		return fmt.Sprintf("jsonb_exists(doc, '%s')", field.text), nil
	}
	if p.acceptWord("NOT") {
		if !p.acceptWord("EXISTS") {
			return "", fmt.Errorf("expected EXISTS after NOT in filter")
		}
		return fmt.Sprintf("NOT jsonb_exists(doc, '%s')", field.text), nil
	}
	// field IS NULL / field IS NOT NULL
	if p.acceptWord("IS") {
		negated := p.acceptWord("NOT")
		if !p.acceptWord("NULL") {
			return "", fmt.Errorf("expected NULL after IS in filter")
		}
		if negated {
			return col + " IS NOT NULL", nil
		}
		return col + " IS NULL", nil
	}

	op := p.next()
	if op.kind != tokOp {
		return "", fmt.Errorf("expected an operator after %q, got %q", field.text, op.text)
	}
	value := p.next()
	if value.kind != tokWord && value.kind != tokString {
		return "", fmt.Errorf("expected a value after %q, got %q", op.text, value.text)
	}
	// argN is the number of the next free positional parameter, so the first
	// value this compiler adds is $argN.
	placeholder := "$" + strconv.Itoa(p.argN)
	p.argN++

	// `=` and `!=` are text comparisons; ranges a numeric value must be a
	// comparison against the number, which is what the API's range filters ask
	// for (snowflake ids and timestamps).
	switch op.text {
	case "=":
		p.args = append(p.args, value.text)
		return col + " IS NOT DISTINCT FROM " + placeholder, nil
	case "!=":
		p.args = append(p.args, value.text)
		return col + " IS DISTINCT FROM " + placeholder, nil
	case ">", ">=", "<", "<=":
		num, err := strconv.ParseFloat(value.text, 64)
		if err != nil {
			return "", fmt.Errorf("range filter on %q needs a number, got %q", field.text, value.text)
		}
		p.args = append(p.args, num)
		return "(doc->>'" + field.text + "')::numeric " + op.text + " " + placeholder, nil
	default:
		return "", fmt.Errorf("unsupported operator %q", op.text)
	}
}

// quoteLiteral renders a validated identifier as a SQL string literal. It only
// exists so the sort compiler can name the jsonb key without concatenating an
// unchecked value into the statement.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
