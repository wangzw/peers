{{- $index := printf "%s\n" (partial "markdown/site-index.md" .) -}}
{{- /* Hugo writes a redirect page at /<default language>/, and that path needs an index.md as well. */ -}}
{{- if site.Language.IsDefault -}}
  {{- $alias := resources.FromString (printf "%s/index.md" site.Language.Lang) $index -}}
  {{- $published := $alias.Publish -}}
{{- end -}}
{{- $index -}}
