package database

import (
	"strings"
)

func QuoteIdent(ident string) string {
	if ident == "" {
		return "``"
	}
	escaped := strings.ReplaceAll(ident, "`", "``")
	return "`" + escaped + "`"
}

func QuoteTable(table string) string {
	parts := strings.Split(table, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, QuoteIdent(p))
	}
	if len(out) == 0 {
		return "``"
	}
	return strings.Join(out, ".")
}

