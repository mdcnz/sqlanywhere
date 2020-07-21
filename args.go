package sqlanywhere

import "strings"

type arg struct {
	start int
	end   int
}

const (
	quote       = '\''
	escape      = '\\'
	param       = ':'
	placeholder = '?'
)

func isArg(b rune) bool {
	return b >= '0' && b <= '9' ||
		b >= 'A' && b <= 'Z' ||
		b >= 'a' && b <= 'z' ||
		b == '_'
}

type split struct {
	sql  []rune
	args []*arg
}

//splitNamed finds named parameters, returning the sql with replacement placeholders
//and a slice of the named parameters.
func splitNamed(sql string) (string, []string) {
	s := &split{
		args: []*arg{},
		sql:  []rune(sql),
	}

	var a *arg
	quoted := false

	for i, c := range s.sql {

		var prior rune
		if i > 0 {
			prior = s.sql[i-1]
		}

		if c == quote && prior != escape {
			quoted = !quoted
		}

		if quoted {
			continue
		}

		var next rune
		if i < len(s.sql)-1 {
			next = s.sql[i+1]
		}

		if c == param && isArg(next) {
			a = &arg{start: i}
			continue
		}

		if a != nil {
			if !isArg(next) {
				a.end = i + 1

				s.args = append(s.args, a)

				a = nil
			}
		}
	}

	return s.collapse(), s.arguments()
}

//collapse returns sql with the named parameters collapsed into placeholders.
func (s *split) collapse() string {
	b := strings.Builder{}
	from := 0
	for _, a := range s.args {
		b.WriteString(string(s.sql[from:a.start]))
		b.WriteByte(placeholder)
		from = a.end
	}

	if from < len(s.sql) {
		b.WriteString(string(s.sql[from:]))
	}

	return b.String()
}

//arguments returns the argument names (excluding the argument token)
func (s *split) arguments() []string {
	arguments := make([]string, len(s.args))
	for i, a := range s.args {
		arguments[i] = string(s.sql[a.start+1 : a.end])
	}
	return arguments
}
