package notify

import (
	"fmt"
	"strings"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type Template struct {
	Title string
	Body  string
}

func (t Template) Render(env map[string]any) (string, string) {
	return interpolate(t.Title, env), interpolate(t.Body, env)
}

func (t Template) RenderAlert(a pipeline.Alert) (string, string) {
	return t.Render(pipeline.AlertEnv(a))
}

func interpolate(s string, env map[string]any) string {
	if !strings.Contains(s, "${") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if i+1 < len(s) && s[i] == '$' && s[i+1] == '{' {
			end := strings.Index(s[i+2:], "}")
			if end == -1 {
				b.WriteByte(s[i])
				i++
				continue
			}
			path := s[i+2 : i+2+end]
			b.WriteString(lookup(env, path))
			i += 2 + end + 1
			continue
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

func lookup(env map[string]any, path string) string {
	parts := strings.Split(path, ".")
	var cur any = env
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	if cur == nil {
		return ""
	}
	return fmt.Sprintf("%v", cur)
}
