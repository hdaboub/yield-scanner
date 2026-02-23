# GitHub Versioning & Branch Protection

## 1) Branch Protection (for `main`)
Use GitHub settings for the `main` branch with these rules:

- Require a pull request before merging
- Require approvals: 1+
- Dismiss stale pull request approvals when new commits are pushed
- Require status checks to pass before merging
- Required checks:
  - `Python 3.11`
  - `Python 3.12`
- Require branches to be up to date before merging
- Restrict force pushes
- Restrict deletions

## 2) CI and Release Workflows
Included workflows:

- `.github/workflows/ci.yml`
  - Runs on push to `main` and on pull requests
  - Compile checks and unit tests

- `.github/workflows/release.yml`
  - Runs on tag push matching `v*`
  - Creates a GitHub Release automatically

## 3) Semantic Versioning Policy
Use `vMAJOR.MINOR.PATCH`:

- MAJOR: breaking changes
- MINOR: backward-compatible features
- PATCH: backward-compatible bug fixes

## 4) Create a Release Tag
From repo root with clean working tree:

```bash
./scripts/release.sh v0.1.0 --push
```

Without immediate push:

```bash
./scripts/release.sh v0.1.0
```

Then push manually:

```bash
git push origin main
git push origin v0.1.0
```
