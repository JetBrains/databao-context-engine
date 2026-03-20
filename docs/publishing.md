# PyPI Publishing

This document describes how to publish a new version of databao-context-engine to PyPI.

## Overview

The project uses GitHub Actions to automatically publish releases to PyPI when a new version tag is pushed. The pipeline
uses `uv` for building and publishing the package.

During development, the `main` branch stays on a dev release version (e.g., `0.7.0.dev0`). When ready to publish a
stable release, the version is bumped to stable, committed, tagged, and automatically published via GitHub Actions.

## Development Workflow

### During Development

- The `main` branch should always have a dev version (e.g., `0.7.1.dev0`)
- All development happens on this dev version
- No manual actions needed during development

### Publishing a New Stable Release

When ready to publish a new stable version:

1. **Bump to stable version**:
   ```bash
   uv version --bump stable
   ```
   This converts `X.Y.Z.dev0` → `X.Y.Z`

2. **Commit and push the version change**:
   ```bash
   git add pyproject.toml uv.lock
   git commit -m "Update version to $(uv version --short)"
   git push origin main
   ```

3. **Create and push a Git tag**:
   ```bash
   git tag -a "v$(uv version --short)" -m "Release $(uv version --short)"
   git push origin --tags
   ```

4. **Monitor the publishing workflow** in
   the [Actions tab](https://github.com/JetBrains/databao-context-engine/actions/workflows/publish_tag_triggered.yml)

5. **After successful publish, start the next development cycle**:
   ```bash
   uv version --bump patch --bump dev
   git add pyproject.toml uv.lock
   git commit -m "Start development on $(uv version --short)"
   git push origin main
   ```
   This creates the next dev version (e.g., `0.7.0` → `0.7.1.dev0`)

### For Breaking Changes

If you've pushed breaking changes during the development cycle:

```bash
uv version --bump minor --bump dev
git add pyproject.toml uv.lock
# but it's obiviously better to commit this together with the breaking changes.
git commit -m "Bump to $(uv version --short) for breaking changes"
git push origin main
```

This ensures the next stable release will have the correct minor version bump.

## Versioning Guidelines

This project follows [Semantic Versioning](https://semver.org/) (X.Y.Z):

- **X (Major)**: Incompatible API changes
- **Y (Minor)**: Backward-compatible new features or **breaking changes** during 0.x development
- **Z (Patch)**: Backward-compatible bug fixes

**Important**: If a breaking change is introduced, at least the minor version (Y) must be incremented. This applies even
for small breaking changes like renaming a public function or changing a method signature.

## Important Notes

- The Git tag version **must** match the version in `pyproject.toml`, otherwise the workflow will fail
- Tags must start with `v` (e.g., `v0.6.2`)
- The pipeline uses PyPI's Trusted Publishing (OIDC), so no manual token configuration is needed:
    - Doesn't require storing API tokens
    - Uses OpenID Connect (OIDC) for authentication
    - Requires the `pypi-publish` GitHub environment to be configured
    - Only works from the specific GitHub repository and workflow

