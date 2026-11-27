{{- /*
  Returns a documentation page as Markdown: its title, a Source line naming its
  absolute URL, a Version line, then its body. It is the page's index.md and
  its block in llms-full.txt.

  RenderShortcodes expands the relref shortcodes while keeping the rest of the
  body as Markdown, so code blocks survive and no shortcode syntax is left
  behind. Site-relative links that relref produces are rewritten to absolute
  URLs, and in-page anchors are rewritten to the absolute URL of the page.
*/ -}}
{{- $base := urls.Parse site.BaseURL -}}
{{- $prefix := $base.Path -}}
{{- $origin := printf "%s://%s" $base.Scheme $base.Host -}}
{{- $version := partial "version/context.html" . -}}

{{- $body := .RenderShortcodes -}}
{{- if $prefix -}}
  {{- $body = replaceRE (printf `\]\(%s` $prefix) (printf "](%s%s" $origin $prefix) $body -}}
{{- end -}}
{{- $body = replaceRE `\]\(#([^)]+)\)` (printf "](%s#$1)" .Permalink) $body -}}
{{- $latest := partial "version/in.html" (dict "base" $version.latestBaseURL "page" .) -}}
{{- $line := "" -}}
{{- if or (eq $version.class "latest") (eq $version.class "latest-copy") -}}
  {{- $line = printf "Version: %s (latest release)" $version.name -}}
{{- else if eq $version.class "older" -}}
  {{- $line = printf "Version: %s (superseded; latest release %s at %s)" $version.name $version.latestName $latest -}}
{{- else -}}
  {{- $line = printf "Version: development (unreleased; latest release %s at %s)" $version.latestName $latest -}}
{{- end -}}
{{- return (printf "# %s\n\nSource: %s\n%s\n\n%s" .Title .Permalink $line ($body | strings.TrimSpace)) -}}
