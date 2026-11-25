{{- /*
  Returns a documentation page as Markdown: its title, a Source line naming its
  absolute URL, then its body. It is the page's index.md and its block in
  llms-full.txt.

  RenderShortcodes expands the relref shortcodes while keeping the rest of the
  body as Markdown, so code blocks survive and no shortcode syntax is left
  behind. Site-relative links that relref produces are rewritten to absolute
  URLs, and in-page anchors are rewritten to the absolute URL of the page.
*/ -}}
{{- $base := urls.Parse site.BaseURL -}}
{{- $prefix := $base.Path -}}
{{- $origin := printf "%s://%s" $base.Scheme $base.Host -}}

{{- $body := .RenderShortcodes -}}
{{- if $prefix -}}
  {{- $body = replaceRE (printf `\]\(%s` $prefix) (printf "](%s%s" $origin $prefix) $body -}}
{{- end -}}
{{- $body = replaceRE `\]\(#([^)]+)\)` (printf "](%s#$1)" .Permalink) $body -}}
{{- return (printf "# %s\n\nSource: %s\n\n%s" .Title .Permalink ($body | strings.TrimSpace)) -}}
