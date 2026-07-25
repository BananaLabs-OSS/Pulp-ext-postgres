package postgresext

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// rewriteSQLiteSQL translates the small, documented SQLite-storage ABI
// surface that existing Pulp WASM cells emit. It is not a general SQLite
// dialect converter: any SQLite-only form outside this surface is rejected so
// a host never executes SQL with altered meaning.
func rewriteSQLiteSQL(query string, argCount int) (string, error) {
	pieces, err := lexSQL(query)
	if err != nil {
		return "", fmt.Errorf("storage.postgres: invalid SQL: %w", err)
	}

	var questionCount, maxNativeParam int
	for i := range pieces {
		switch pieces[i].kind {
		case sqlQuestion:
			questionCount++
		case sqlNativeParam:
			n, _ := strconv.Atoi(pieces[i].text[1:])
			if n > maxNativeParam {
				maxNativeParam = n
			}
		}
	}
	if questionCount > 0 && maxNativeParam > 0 {
		return "", fmt.Errorf("storage.postgres: mixed SQLite ? and Postgres $n parameters are ambiguous")
	}
	if questionCount > 0 && questionCount != argCount {
		return "", fmt.Errorf("storage.postgres: %d SQLite placeholders for %d arguments", questionCount, argCount)
	}
	if maxNativeParam > argCount {
		return "", fmt.Errorf("storage.postgres: highest Postgres parameter is $%d but only %d arguments were supplied", maxNativeParam, argCount)
	}

	n := 0
	for i := range pieces {
		if pieces[i].kind == sqlQuestion {
			n++
			pieces[i].text = "$" + strconv.Itoa(n)
		}
	}
	if err := rewriteSQLiteDDL(pieces); err != nil {
		return "", err
	}
	if err := rejectSQLiteOnly(pieces); err != nil {
		return "", err
	}
	if err := rewriteHexLiterals(pieces); err != nil {
		return "", err
	}

	var out strings.Builder
	for _, piece := range pieces {
		out.WriteString(piece.text)
	}
	return out.String(), nil
}

type sqlPieceKind uint8

const (
	sqlWord sqlPieceKind = iota
	sqlSpace
	sqlSymbol
	sqlQuestion
	sqlNativeParam
	sqlSingleQuoted
	sqlDoubleQuoted
	sqlComment
)

type sqlPiece struct {
	kind sqlPieceKind
	text string
}

// lexSQL preserves every byte while making only code tokens eligible for
// compatibility rewriting. Strings, quoted identifiers, and comments are
// opaque, which prevents a literal or comment from becoming executable SQL.
func lexSQL(query string) ([]sqlPiece, error) {
	pieces := make([]sqlPiece, 0, len(query)/2)
	for i := 0; i < len(query); {
		start := i
		switch {
		case isSQLSpace(query[i]):
			for i < len(query) && isSQLSpace(query[i]) {
				i++
			}
			pieces = append(pieces, sqlPiece{sqlSpace, query[start:i]})
		case strings.HasPrefix(query[i:], "--"):
			i += 2
			for i < len(query) && query[i] != '\n' {
				i++
			}
			pieces = append(pieces, sqlPiece{sqlComment, query[start:i]})
		case strings.HasPrefix(query[i:], "/*"):
			i += 2
			depth := 1
			for i < len(query) && depth > 0 {
				switch {
				case strings.HasPrefix(query[i:], "/*"):
					depth++
					i += 2
				case strings.HasPrefix(query[i:], "*/"):
					depth--
					i += 2
				default:
					i++
				}
			}
			if depth != 0 {
				return nil, fmt.Errorf("unterminated block comment")
			}
			pieces = append(pieces, sqlPiece{sqlComment, query[start:i]})
		case query[i] == '\'':
			end, err := scanQuoted(query, i, '\'')
			if err != nil {
				return nil, err
			}
			i = end
			pieces = append(pieces, sqlPiece{sqlSingleQuoted, query[start:i]})
		case query[i] == '"':
			end, err := scanQuoted(query, i, '"')
			if err != nil {
				return nil, err
			}
			i = end
			pieces = append(pieces, sqlPiece{sqlDoubleQuoted, query[start:i]})
		case query[i] == '$' && isDollarQuoteStart(query, i):
			end, err := scanDollarQuoted(query, i)
			if err != nil {
				return nil, err
			}
			i = end
			pieces = append(pieces, sqlPiece{sqlSingleQuoted, query[start:i]})
		case query[i] == '`':
			return nil, fmt.Errorf("SQLite backtick quoted identifiers are unsupported")
		case query[i] == '[':
			return nil, fmt.Errorf("SQLite bracket quoted identifiers are unsupported")
		case query[i] == '$' && i+1 < len(query) && query[i+1] >= '0' && query[i+1] <= '9':
			if query[i+1] == '0' {
				return nil, fmt.Errorf("$0 is not a valid Postgres parameter")
			}
			i += 2
			for i < len(query) && query[i] >= '0' && query[i] <= '9' {
				i++
			}
			pieces = append(pieces, sqlPiece{sqlNativeParam, query[start:i]})
		case query[i] == '?':
			if i+1 < len(query) && (query[i+1] == '|' || query[i+1] == '&' || query[i+1] == '?') {
				return nil, fmt.Errorf("Postgres JSON ? operator is unsupported with SQLite-storage compatibility")
			}
			if i+1 < len(query) && query[i+1] >= '0' && query[i+1] <= '9' {
				return nil, fmt.Errorf("SQLite numbered ?NNN parameters are unsupported; use unnumbered ? parameters")
			}
			i++
			pieces = append(pieces, sqlPiece{sqlQuestion, query[start:i]})
		case isSQLWordStart(query[i]):
			i++
			for i < len(query) && isSQLWordContinue(query[i]) {
				i++
			}
			pieces = append(pieces, sqlPiece{sqlWord, query[start:i]})
		default:
			i++
			pieces = append(pieces, sqlPiece{sqlSymbol, query[start:i]})
		}
	}
	return pieces, nil
}

func scanQuoted(query string, start int, quote byte) (int, error) {
	for i := start + 1; i < len(query); i++ {
		if query[i] != quote {
			continue
		}
		if i+1 < len(query) && query[i+1] == quote {
			i++
			continue
		}
		return i + 1, nil
	}
	return 0, fmt.Errorf("unterminated quoted SQL token")
}

func isDollarQuoteStart(query string, start int) bool {
	end := start + 1
	for end < len(query) && (isSQLWordStart(query[end]) || (query[end] >= '0' && query[end] <= '9')) {
		end++
	}
	return end < len(query) && query[end] == '$'
}

func scanDollarQuoted(query string, start int) (int, error) {
	endTag := start + 1
	for endTag < len(query) && (isSQLWordStart(query[endTag]) || (query[endTag] >= '0' && query[endTag] <= '9')) {
		endTag++
	}
	tag := query[start : endTag+1]
	if end := strings.Index(query[endTag+1:], tag); end >= 0 {
		return endTag + 1 + end + len(tag), nil
	}
	return 0, fmt.Errorf("unterminated dollar-quoted SQL token")
}

func isSQLSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f'
}

func isSQLWordStart(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isSQLWordContinue(b byte) bool {
	return isSQLWordStart(b) || (b >= '0' && b <= '9') || b == '$'
}

func nextCodeWord(pieces []sqlPiece, from int) int {
	for i := from; i < len(pieces); i++ {
		if pieces[i].kind == sqlWord {
			return i
		}
		if pieces[i].kind == sqlSymbol && pieces[i].text == ";" {
			return -1
		}
	}
	return -1
}

func equalWord(piece sqlPiece, word string) bool {
	return piece.kind == sqlWord && strings.EqualFold(piece.text, word)
}

func rejectSQLiteOnly(pieces []sqlPiece) error {
	for i := range pieces {
		if pieces[i].kind != sqlWord {
			continue
		}
		switch strings.ToUpper(pieces[i].text) {
		case "PRAGMA", "VACUUM", "ATTACH", "DETACH", "REPLACE", "VIRTUAL":
			return fmt.Errorf("storage.postgres: SQLite-only %s is unsupported", strings.ToUpper(pieces[i].text))
		case "AUTOINCREMENT":
			return fmt.Errorf("storage.postgres: AUTOINCREMENT is supported only as INTEGER PRIMARY KEY AUTOINCREMENT")
		case "INSERT":
			first := nextCodeWord(pieces, i+1)
			second := nextCodeWord(pieces, first+1)
			if first >= 0 && second >= 0 && equalWord(pieces[first], "OR") &&
				(equalWord(pieces[second], "IGNORE") || equalWord(pieces[second], "REPLACE")) {
				return fmt.Errorf("storage.postgres: SQLite INSERT OR %s is unsupported; use ON CONFLICT", strings.ToUpper(pieces[second].text))
			}
		}
	}
	return nil
}

func rewriteSQLiteDDL(pieces []sqlPiece) error {
	for i := range pieces {
		if equalWord(pieces[i], "BLOB") {
			pieces[i].text = "BYTEA"
		}
	}
	for i := range pieces {
		if !equalWord(pieces[i], "INTEGER") {
			continue
		}
		primary := nextCodeWord(pieces, i+1)
		key := nextCodeWord(pieces, primary+1)
		autoincrement := nextCodeWord(pieces, key+1)
		if primary < 0 || key < 0 || autoincrement < 0 || !equalWord(pieces[primary], "PRIMARY") || !equalWord(pieces[key], "KEY") || !equalWord(pieces[autoincrement], "AUTOINCREMENT") {
			continue
		}
		pieces[i].text = "BIGSERIAL"
		pieces[autoincrement].text = ""
	}
	return nil
}

func rewriteHexLiterals(pieces []sqlPiece) error {
	for i := 0; i+1 < len(pieces); i++ {
		if !equalWord(pieces[i], "X") || pieces[i+1].kind != sqlSingleQuoted {
			continue
		}
		hex := pieces[i+1].text[1 : len(pieces[i+1].text)-1]
		if len(hex)%2 != 0 || !isHex(hex) {
			return fmt.Errorf("storage.postgres: invalid SQLite X'hex' literal")
		}
		pieces[i].text = "decode("
		pieces[i+1].text = "'" + hex + "', 'hex')"
	}
	return nil
}

func isHex(s string) bool {
	for _, r := range s {
		if !unicode.Is(unicode.ASCII_Hex_Digit, r) {
			return false
		}
	}
	return true
}
