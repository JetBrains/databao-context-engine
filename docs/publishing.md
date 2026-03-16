# PyPI Publishing

This document describes how to publish a new version of databao-context-engine to PyPI.

## Overview

The project uses GitHub Actions to automatically publish releases to PyPI when a new version tag is pushed. The pipeline
uses `uv` for building and publishing the package.

## Trigger

The publishing workflow is triggered by pushing a Git tag that starts with `v`:

```bash
git tag v0.6.2
git push origin v0.6.2
```

See `.github/workflows/publish.yml` for the trigger configuration.

## How to Publish a New Version

1. Update the version in `pyproject.toml` (version attribute). This could be done either manually or by using `uv`:

```bash
uv version --bump patch 
```

See `uv version --help` for more details

2. Commit and **push** the version change. Don't forget to include the updated `uv.lock` file:
   ```bash
   git add .
   git commit -m "Update version to X.Y.Z"
   git push origin main
   ```

3. **Create a Git tag** matching the version (prefixed with `v`):
   ```bash
   git tag -a vX.Y.Z -m vX.Y.Z 
   ```

4. **Push the commit and tag** to GitHub:
   ```bash
   git push origin vX.Y.Z
   ```

5. **Monitor the workflow** in
   the [Actions tab](https://github.com/JetBrains/databao-context-engine/actions/workflows/publish.yml)

GitHub Actions will automatically build and publish the package to PyPI.

## Important Notes

- The Git tag version **must** match the version in `pyproject.toml`, otherwise the workflow will fail
- Tags must start with `v` (e.g., `v0.6.2`)
- The pipeline uses PyPI's Trusted Publishing (OIDC), so no manual token configuration is needed:
    - Doesn't require storing API tokens
    - Uses OpenID Connect (OIDC) for authentication
    - Requires the `pypi-publish` GitHub environment to be configured
    - Only works from the specific GitHub repository and workflow

