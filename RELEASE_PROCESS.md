# Release Process Guide

This document describes the release process for the Impresso consolidated
canonical processing pipeline.

The process is intentionally merge-first and reproducibility-focused: release
notes and final release metadata must be committed and merged to `main` before
the git tag is created, so the tagged repository state, `main`, and the GitHub
release description describe the same snapshot.

## Release Workflow

1. **Review**: compare the release branch with the previous tag.
2. **Prepare**: update documentation, configuration examples, and changelog.
3. **Verify**: run local sample checks and dry-run Make targets before any S3 run.
4. **Document**: write release notes in a committed markdown file.
5. **Merge**: merge the release branch to `main`.
6. **Tag**: create an annotated git tag from the merged commit on `main`.
7. **Publish**: create the GitHub release from the committed release notes file.
8. **Follow up**: announce the release and monitor the first production runs.

The normal path is:

1. prepare release changes on a branch,
2. merge that branch through a pull request,
3. switch to the updated `main`,
4. tag the merged commit on `main`,
5. publish the GitHub release from that tag.

Do not create release tags from feature branches. Do not write or substantially
revise release notes after creating the tag. If a published release note needs
correction, edit the committed release note file, merge that correction to
`main`, and use `gh release edit --notes-file`, but treat that as an exception.

## Version Numbering

Use Semantic Versioning for repository releases:

```text
MAJOR.MINOR.PATCH
```

- **MAJOR**: incompatible pipeline or configuration changes.
- **MINOR**: backwards-compatible pipeline features, new input kinds, or new
  supported media workflows.
- **PATCH**: backwards-compatible fixes, documentation corrections, or narrowly
  scoped validation fixes.

Examples:

- `v1.0.0` -> `v2.0.0`: breaking change to required configuration variables or
  output layout.
- `v1.0.0` -> `v1.1.0`: new radio/audio input support.
- `v1.1.0` -> `v1.1.1`: bug fix in validation or path handling.

Pre-release tags may be used for trial runs:

- `v1.1.0-alpha.1`
- `v1.1.0-beta.1`
- `v1.1.0-rc.1`

## Preparing A Release

### 1. Review Changes

Compare with the previous release tag:

```bash
git log v1.0.0..HEAD --oneline
git diff v1.0.0..HEAD --stat
git diff v1.0.0..HEAD --name-status
```

Review changes by area:

```bash
git log v1.0.0..HEAD --oneline -- lib/
git log v1.0.0..HEAD --oneline -- cookbook/
git log v1.0.0..HEAD --oneline -- configs/
git log v1.0.0..HEAD --oneline -- README.md
```

Check the working tree before preparing the release commit:

```bash
git status --short
```

Local build output, credentials, and private configuration must not be included in
the release commit.

Confirm the release branch target is `main` and that the final tag will be
created only after the release branch has been merged:

```bash
git branch --show-current
git fetch origin
git log origin/main..HEAD --oneline
```

### 2. Update Documentation

- [ ] Update `CHANGELOG.md` or create it if this release starts the repository
      changelog.
- [ ] Update `README.md` for user-facing behavior changes.
- [ ] Keep public examples written with `make`.
- [ ] Review `config.sample.mk` and checked-in files under `configs/`.
- [ ] Update any release-specific run IDs or output versions.
- [ ] Document known limitations and validation requirements.

For radio/audio releases, also verify that documentation covers:

- [ ] `CANONICAL_INPUT_KIND := audios`.
- [ ] canonical audio input layout under `audios/`.
- [ ] consolidated audio output layout under `audios/`.
- [ ] the langident/OCRQA run ID used for audio enrichment.
- [ ] whether schema validation is required for the release.

### 3. Update Version And Configuration References

Check and update:

- [ ] `configs/config_consolidatedcanonical_*.mk`.
- [ ] `config.sample.mk`.
- [ ] README examples that mention `RUN_VERSION_CONSOLIDATEDCANONICAL`.
- [ ] README examples that mention `LANGIDENT_ENRICHMENT_RUN_ID`.
- [ ] release notes links and full changelog links.

This repository does not publish a standalone Python package from the top-level
pipeline. Do not add package-version changes unless the release explicitly changes
the packaged cookbook helper library.

### 4. Verify The Release

Run the fast local CLI smoke test:

```bash
python3 lib/cli_consolidatedcanonical.py \
  --canonical-input test_data/sample_canonical_issue.jsonl \
  --enrichment-input test_data/sample_enrichment.jsonl \
  --output test_data/output_consolidated.jsonl \
  --langident-run-id langident-lid-ensemble_multilingual_v2-0-2 \
  --metadata-json metadata/corpus_access_catalogue.json \
  --log-level INFO
```

Run syntax checks:

```bash
python3 -m py_compile lib/cli_consolidatedcanonical.py lib/cli_pages_tx_diff.py
```

Run dry-run Make checks before any S3-affecting target:

```bash
make -n newspaper PROVIDER=BL NEWSPAPER=WTCH
make -n newspaper \
  CFG=configs/config_consolidatedcanonical_v2026-05-26_audio.mk \
  PROVIDER=RTS \
  NEWSPAPER=RTS/ana_media
```

For a radio/audio release, perform at least one controlled dry run with the audio
configuration and record the exact provider, source, output version, and
langident run ID in the release notes.

Only run S3 sync, processing, or upload targets when credentials and target
bucket/version settings have been confirmed.

## Creating Release Notes

Create a release notes file before tagging:

```text
RELEASE_NOTES_vX.Y.Z.md
```

Release notes should include:

1. Overview.
2. Major features.
3. Pipeline and configuration changes.
4. Validation and test evidence.
5. Breaking changes, if any.
6. Migration notes.
7. Known issues.
8. Contributors.

Template:

```markdown
# Release Notes - vX.Y.Z

**Release Date:** YYYY-MM-DD
**Tag:** vX.Y.Z
**Status:** Stable

## Overview

Brief summary of the release.

## Major Features

- New feature summary.

## Pipeline Changes

- Make, path, sync, or processing changes.

## Configuration

- New or changed configuration files and variables.

## Verification

- Commands run and results.
- S3 dry-run or production run evidence, when applicable.

## Breaking Changes

- None, or describe compatibility impact and migration path.

## Migration Notes

- How existing users should update configs or commands.

## Known Issues

- Known limitations.

## Links

- Full Changelog: https://github.com/impresso/impresso-consolidated-canonical-cookbook/compare/vPREVIOUS...vX.Y.Z

## Contributors

- Contributor names or handles.
```

Generate change lists with:

```bash
git log v1.0.0..HEAD --oneline
git shortlog v1.0.0..HEAD -sn
git diff v1.0.0..HEAD --stat
git diff v1.0.0..HEAD --name-status
```

## Publishing A Release

### 1. Commit Release Notes And Final Metadata

Before tagging, commit the release notes and final documentation/configuration
updates on the release branch:

```bash
git add CHANGELOG.md README.md RELEASE_NOTES_v1.1.0.md configs/
git commit -m "Prepare release v1.1.0"
```

Adjust the paths to match the actual release changes. Do not add private local
configuration, credentials, or transient build output.

### 2. Merge To Main

Open a pull request for the release branch and merge it to `main`. After the pull
request is merged, update the local `main` branch:

```bash
git checkout main
git pull --ff-only origin main
```

Confirm that `main` contains the release notes and intended release commit:

```bash
git log -1 --oneline
test -f RELEASE_NOTES_v1.1.0.md
```

### 3. Create The Git Tag From Main

```bash
git checkout main
git tag -a v1.1.0 -m "Release v1.1.0: radio audio integration"
git push origin v1.1.0
```

### 4. Create The GitHub Release

Use the committed notes file:

```bash
gh release create v1.1.0 \
  --repo impresso/impresso-consolidated-canonical-cookbook \
  --title "v1.1.0: Radio audio integration" \
  --notes-file RELEASE_NOTES_v1.1.0.md
```

For pre-releases, add `--prerelease`.

### 5. Correct An Existing Release If Needed

```bash
gh release edit v1.1.0 \
  --repo impresso/impresso-consolidated-canonical-cookbook \
  --notes-file RELEASE_NOTES_v1.1.0.md
```

## Post-Release Tasks

- [ ] Confirm the release tag is visible on GitHub.
- [ ] Confirm the release tag points to a commit reachable from `main`.
- [ ] Confirm the GitHub release description matches the committed release notes.
- [ ] Ensure release documentation is present on the main branch.
- [ ] Notify maintainers and pipeline users.
- [ ] Record any first production run commands and output locations.
- [ ] Monitor GitHub issues and the first S3 processing/upload logs.

## Hotfix Releases

For critical fixes after a release:

1. Create a hotfix branch from the release tag:

   ```bash
   git checkout -b hotfix/v1.1.1 v1.1.0
   ```

2. Apply the fix and run focused verification.

3. Commit the hotfix release notes before tagging.

4. Create and push an annotated patch tag:

   ```bash
   git tag -a v1.1.1 -m "Release v1.1.1: validation hotfix"
   git push origin v1.1.1
   ```

5. Publish the GitHub release from the committed hotfix notes.

6. Merge the hotfix branch back to `main`.

## Release Checklist

- [ ] Release version selected.
- [ ] Previous tag identified.
- [ ] `git log`, `git diff --stat`, and `git diff --name-status` reviewed.
- [ ] Working tree checked for unrelated or private files.
- [ ] Documentation updated.
- [ ] Configuration examples updated.
- [ ] Changelog updated or intentionally deferred with rationale.
- [ ] Release notes written and committed before tagging.
- [ ] Local CLI smoke test run.
- [ ] Python syntax check run.
- [ ] Relevant Make dry runs completed.
- [ ] S3-affecting commands reviewed before execution.
- [ ] Release branch merged to `main`.
- [ ] Local `main` updated with `git pull --ff-only origin main`.
- [ ] Annotated tag created from `main`.
- [ ] Tag pushed.
- [ ] GitHub release created from committed notes.
- [ ] Team notified.
- [ ] First production run monitored.

## Resources

- GitHub CLI: https://cli.github.com/
- Semantic Versioning: https://semver.org/
- Keep a Changelog: https://keepachangelog.com/
- Git Tagging: https://git-scm.com/book/en/v2/Git-Basics-Tagging

---

**Last Updated:** May 30, 2026
