{{- /*
  Returns the documentation index of the current language as Markdown,
  following https://llmstxt.org/: the body of llms.txt and of the home page's
  index.md.

  After the summary it carries a Version line. A latest build also ends with an
  Other versions section linking each other version's llms.txt in this
  language.

  The page list is generated from the documentation section, so a new page
  appears here without editing this file. Groups follow the sidebar order:
  the top-level pages that come before the first subsection, then one group
  per subsection in weight order, then the top-level pages that come after the
  last subsection. Each line carries the page's front matter description; a
  page without one fails the build, and the summary line comes from
  site.Params.description.
*/ -}}
{{- $page := . -}}
{{- $version := partial "version/context.html" . -}}
{{- $latest := partial "version/in.html" (dict "base" $version.latestBaseURL "page" $page) -}}
{{- $line := "" -}}
{{- if or (eq $version.class "latest") (eq $version.class "latest-copy") -}}
  {{- $line = printf "Version: %s (latest release)" $version.name -}}
{{- else if eq $version.class "older" -}}
  {{- $line = printf "Version: %s (superseded; latest release %s at %s)" $version.name $version.latestName $latest -}}
{{- else -}}
  {{- $line = printf "Version: development (unreleased; latest release %s at %s)" $version.latestName $latest -}}
{{- end -}}
{{- $labels := site.Params.llms | default dict -}}
{{- $docsLabel := index $labels "documentation" | default "Documentation" -}}
{{- $additionalLabel := index $labels "additional" | default "More documentation" -}}
{{- $moreLabel := index $labels "more" | default "More" -}}
{{- $fullLabel := index $labels "full" | default "The full documentation as one plain-text file." -}}
{{- $repoLabel := index $labels "repository" | default "peers on GitHub" -}}

{{- $docs := site.GetPage "/docs" -}}
{{- /* Split the top-level pages into those before the first subsection and
       those after the last one, keeping the sidebar's weight order. */ -}}
{{- $before := slice $docs -}}
{{- $sections := slice -}}
{{- $after := slice -}}
{{- $seenSection := false -}}
{{- range $docs.Pages.ByWeight -}}
  {{- if .IsSection -}}
    {{- $seenSection = true -}}
    {{- $pages := slice . -}}
    {{- range .Pages.ByWeight -}}
      {{- $pages = $pages | append . -}}
    {{- end -}}
    {{- $sections = $sections | append (dict "title" (.LinkTitle | default .Title) "pages" $pages) -}}
  {{- else if $seenSection -}}
    {{- $after = $after | append . -}}
  {{- else -}}
    {{- $before = $before | append . -}}
  {{- end -}}
{{- end -}}

{{- $groups := slice (dict "title" $docsLabel "pages" $before) -}}
{{- range $sections -}}
  {{- $groups = $groups | append . -}}
{{- end -}}
{{- if $after -}}
  {{- $groups = $groups | append (dict "title" $additionalLabel "pages" $after) -}}
{{- end -}}

{{- $out := slice -}}
{{- $out = $out | append (printf "# %s" site.Title) -}}
{{- $out = $out | append (printf "> %s" site.Params.description) -}}
{{- $out = $out | append $line -}}
{{- range $groups -}}
  {{- $lines := slice -}}
  {{- range .pages -}}
    {{- if not .Description -}}
      {{- errorf "llms.txt: page %q has no front matter description" .Path -}}
    {{- end -}}
    {{- $lines = $lines | append (printf "- [%s](%s): %s" .Title .Permalink .Description) -}}
  {{- end -}}
  {{- $out = $out | append (printf "## %s\n\n%s" .title (delimit $lines "\n")) -}}
{{- end -}}
{{- $links := slice -}}
{{- $links = $links | append (printf "- [llms-full.txt](%sllms-full.txt): %s" site.Home.Permalink $fullLabel) -}}
{{- $links = $links | append (printf "- [%s](https://github.com/wangzw/peers)" $repoLabel) -}}
{{- $out = $out | append (printf "## %s\n\n%s" $moreLabel (delimit $links "\n")) -}}
{{- if eq $version.class "latest" -}}
  {{- $others := slice -}}
  {{- range $version.versions -}}
    {{- if ne .name $version.name -}}
      {{- $url := partial "version/in.html" (dict "base" .url "page" $page "format" "llms") -}}
      {{- $others = $others | append (printf "- [%s](%s)" .name $url) -}}
    {{- end -}}
  {{- end -}}
  {{- if $others -}}
    {{- $out = $out | append (printf "## %s\n\n%s" "Other versions" (delimit $others "\n")) -}}
  {{- end -}}
{{- end -}}
{{- return (delimit $out "\n\n") -}}
