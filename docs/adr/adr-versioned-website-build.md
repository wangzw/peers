# Rebuild every published release of the website from git on each deploy

## Context

The website under `docs/site/` is a Hugo site deployed to GitHub Pages by `.github/workflows/site.yml`. A single build of main's content documents behaviour a reader running an earlier release does not have. Every published patch release from v0.6.1 on is a version a reader can have installed, and the unreleased main is a further version the documentation shows.

- GitHub Pages serves one uploaded artifact per deployment, sets no response headers and performs no server-side redirect, so every version is in one artifact.
- Existing links address the site root: `README.md`, `.github/ISSUE_TEMPLATE/config.yml`, external pages, and the Cloudflare rule documented in `docs/site/hugo.yaml` that answers a request with `Accept: text/markdown` with the page's `index.md`.
- The signals that tell search engines and agents which version a page belongs to (robots meta, canonical, banner, version selector, `Version:` lines; [ADR-versioned-website-indexing](adr-versioned-website-indexing.md)) are rendered by the templates, and an earlier version's signals depend on releases published after it: the selector lists them and the banner names the current latest.
- The templates and configuration at v0.6.1's tag carry none of those signals, and its site content was aligned with the release only in a commit after the tag.
- Pages take their dates from git history (`enableGitInfo` in `docs/site/hugo.yaml`), which needs each version's full history.
- The `github-pages` environment accepts deployments from main only.

## Options Considered

### Build one site from main

**Rejected**: a reader of an earlier release reads documentation of behaviour that release does not have, which is the disagreement the decision removes.

### Store built snapshots

Build each release once when it is published and keep the output, on a `gh-pages` branch or as release assets, adding only the new release at each deploy.

**Rejected**: a stored snapshot keeps the selector, banner and indexing signals of the deploy that built it, so an earlier release neither lists later releases nor names the current latest, and a template correction never reaches it. Changing that requires rebuilding the snapshots.

### Build each release with its own templates

Check out each release and build its whole `docs/site/` as it was.

**Rejected**: the templates at v0.6.1 emit no version signals, and every later release would keep the signals of its own time, so the versions in one deployment would disagree on robots meta, canonical and selector.

### Serve the latest release at /latest/

Put every version under its own path, the latest included, and send the root to `/latest/`.

**Rejected**: GitHub Pages has no server-side redirect, so the root would answer with a client-side redirect page; existing links and the Markdown rewrite rule address the root, and a crawler or agent that does not follow that page gets no content there.

### Rebuild every published release from git on each deploy

**Adopted**; see Decision.

## Decision

Adopt a rebuild of every published release on each deploy of the website, keeping nothing built between deploys:

- The deployed versions are the `vX.Y.Z` tags at or after the oldest release the repository records for the website that have a published GitHub release, neither a draft nor a prerelease. Checks build every `vX.Y.Z` tag from the oldest on. The latest is the highest by semantic version.
- Each version takes its site content from its own ref, which is its tag unless the repository names another commit for it, checked out with full history. Every other website file, templates, configuration and shared assets included, comes from the deploying commit, so one template set renders every version.
- The latest release is served at the root and at `/vX.Y.Z/`, every other published release at `/vX.Y.Z/`, and the deploying commit's own content at `/dev/`. The files served at the host root come from the root build.
- The Site workflow deploys from main on a push to main and after every successful run of the Release workflow; a pull request that touches the website builds and checks every version without deploying.

## Consequences

A release's pages keep that release's content while their templates, metadata and signals follow the latest deploy. A version becomes the default only after its GitHub release is published; a tag whose Release run fails is never deployed.

**Costs accepted:**

- Deploy and check time grow with the number of published releases: every deploy and every pull request that touches the website builds each one.
- A template or configuration change must render the content of every published release. A change that needs something an earlier release's content lacks, such as a front matter field or a shortcode, cannot land until it handles that content, because an earlier release's content is never edited.
- A deploy reads the list of published releases from GitHub, and every build needs the repository's full history and tags; a shallow clone or an export without history cannot run it.
- A release whose aligned site content lands after its tag needs an entry in `docs/site/versions.yaml` naming that commit.
- One Hugo version and one theme version build every release; upgrading either must build all of them.

## Invalidation Conditions

The decision holds while one Site workflow run builds every published release. If that build outgrows the job time limit of GitHub Actions, or a template change can no longer render an earlier release's content without editing it, storing or freezing earlier builds has to be weighed again.
