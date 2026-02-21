# PyPI Release Checklist for pysof

This checklist ensures high-quality PyPI releases with proper metadata, documentation, and functionality.

## Current Status (tech/pysof branch)

**Completed Preparation Work:**
- ✅ Version synchronized to 0.1.25 across all files
- ✅ Project URLs fixed (using [project.urls] table, pointing to pysof crate)
- ✅ README restructured (badges, quick links, user-first approach)
- ✅ Python version support expanded (3.10 - 3.14)
- ✅ All metadata and classifiers updated
- ✅ Documentation updated (RELEASING.md, this checklist)

**Ready For:**
- ⏳ Building and testing wheels
- ⏳ TestPyPI upload and verification
- ⏳ Production PyPI release

---

## Pre-Release Checklist

### Version Synchronization
- [x] Workspace version updated in `Cargo.toml` (root) - v0.1.25
- [x] `crates/pysof/Cargo.toml` uses `version.workspace = true`
- [x] `crates/pysof/pyproject.toml` version matches workspace version - v0.1.25
- [ ] Verify versions match:
  ```bash
  grep "^version" Cargo.toml
  grep "^version" crates/pysof/pyproject.toml
  ```

### Code Quality
- [ ] All tests passing: `uv run pytest python-tests/`
- [ ] Rust tests passing: `cargo test -p pysof`
- [ ] Code formatted: `cargo fmt --all`
- [ ] No compiler warnings: `cargo build -p pysof --release`
- [ ] Linting clean: `uv run ruff check src/`

### Documentation
- [x] README.md updated with any new features - Restructured with badges, quick links, user-first approach
- [x] Badges display correctly (PyPI version will update after release)
- [x] Quick Start example works - Added prominent quick start section
- [x] All code examples are valid Python
- [ ] CHANGELOG or release notes prepared (if applicable) - TODO: Create for v0.1.25
- [x] Links in README are not broken

### Metadata Validation
- [x] `pyproject.toml` project URLs are correct:
  - [x] Homepage - Points to pysof crate directory
  - [x] Repository - Points to hfs root
  - [x] Documentation - Points to pysof crate directory
  - [x] Bug Tracker - Points to hfs issues
  - [x] Source - Points to pysof crate directory
- [x] Python version requirement correct: `requires-python = ">=3.10"`
- [x] Keywords appropriate for PyPI search
- [x] Classifiers accurate (Development Status, Intended Audience, etc.) - Added Python 3.10-3.14
- [x] License field matches repository license

### Build Verification
- [ ] Clean build succeeds:
  ```bash
  cd crates/pysof
  rm -rf dist/
  uv run maturin build --release -o dist
  ```
- [ ] Source distribution builds:
  ```bash
  uv run maturin sdist -o dist
  ```
- [ ] Inspect wheel metadata:
  ```bash
  unzip -p dist/pysof-*.whl */METADATA | head -50
  ```
- [ ] Verify project URLs appear in METADATA
- [ ] Check wheel contains all necessary files

## TestPyPI Upload (Recommended)

Test the release on TestPyPI before production:

- [ ] Upload to TestPyPI:
  ```bash
  twine upload --repository testpypi dist/*
  ```
- [ ] Visit TestPyPI page: https://test.pypi.org/project/pysof/
  - [ ] Version number correct
  - [ ] README renders correctly
  - [ ] All badges display (may not work on TestPyPI)
  - [ ] Project links visible in sidebar
  - [ ] Classifiers display correctly
  - [ ] License shown correctly
  
- [ ] Test installation from TestPyPI:
  ```bash
  python -m venv test-env
  source test-env/bin/activate  # Windows: test-env\Scripts\activate
  pip install --index-url https://test.pypi.org/simple/ --no-deps pysof
  ```

- [ ] Verify installation:
  ```bash
  python -c "import pysof; print(pysof.__version__)"
  python -c "import pysof; print(pysof.get_supported_fhir_versions())"
  ```

- [ ] Run basic functionality test:
  ```python
  import pysof
  
  view = {
      "resourceType": "ViewDefinition",
      "id": "test",
      "name": "Test",
      "status": "active",
      "resource": "Patient",
      "select": [{"column": [{"name": "id", "path": "id"}]}]
  }
  
  bundle = {
      "resourceType": "Bundle",
      "type": "collection",
      "entry": [{
          "resource": {"resourceType": "Patient", "id": "test-123"}
      }]
  }
  
  result = pysof.run_view_definition(view, bundle, "json")
  print(f"Success! Got {len(result)} bytes")
  ```

## Production PyPI Upload

Only proceed after TestPyPI verification:

- [ ] Clean previous test uploads:
  ```bash
  rm -rf test-env/
  ```

- [ ] Upload to PyPI:
  ```bash
  twine upload dist/*
  ```

- [ ] Verify upload success (may take a few minutes to appear)

## Post-Release Verification

### PyPI Page Quality
- [ ] Visit https://pypi.org/project/pysof/
- [ ] Version number is correct
- [ ] README renders beautifully:
  - [ ] Badges display correctly
  - [ ] Code blocks have syntax highlighting
  - [ ] Tables render properly
  - [ ] Emoji display (or gracefully degrade)
  - [ ] Links are clickable
- [ ] Project links visible in left sidebar:
  - [ ] Homepage
  - [ ] Repository
  - [ ] Documentation
  - [ ] Bug Tracker
  - [ ] Source
- [ ] All project links work when clicked
- [ ] Classifiers display correctly
- [ ] License badge shows "MIT"
- [ ] Download files section shows wheels for all platforms:
  - [ ] Linux x86_64 (glibc)
  - [ ] Linux x86_64 (musl)
  - [ ] Windows x86_64
  - [ ] macOS AArch64 (Apple Silicon)
  - [ ] Source distribution (.tar.gz)

### Installation Testing
- [ ] Create fresh virtual environment:
  ```bash
  python -m venv fresh-test
  source fresh-test/bin/activate  # Windows: fresh-test\Scripts\activate
  ```

- [ ] Install from PyPI:
  ```bash
  pip install pysof
  ```

- [ ] Verify installation:
  ```bash
  pip show pysof
  python -c "import pysof; print(f'Version: {pysof.__version__}')"
  python -c "import pysof; print(f'FHIR versions: {pysof.get_supported_fhir_versions()}')"
  ```

- [ ] Run complete example from README Quick Start
- [ ] Test error handling
- [ ] Test with invalid inputs

### GitHub Integration
- [ ] GitHub Release created with correct tag
- [ ] Release notes match changes
- [ ] All wheel artifacts attached to GitHub Release
- [ ] Source code tarball available

### Documentation Updates
- [ ] Update main README.md if pysof section needs updates
- [ ] Update any installation guides
- [ ] Announce release (optional):
  - [ ] GitHub Discussions
  - [ ] Social media
  - [ ] FHIR community channels

## Rollback Plan

If critical issues are found after release:

1. **Do NOT delete the PyPI release** (this breaks installs for users)
2. **Yank the version on PyPI** if it's broken:
   ```bash
   # This prevents new installations but keeps existing ones working
   # Visit PyPI project page and use "Yank release" option
   ```
3. **Publish a patch version** with fixes as soon as possible
4. **Document the issue** in release notes

## Common Issues and Solutions

### Project Links Not Showing
- **Problem**: URLs in `pyproject.toml` not appearing on PyPI
- **Solution**: Ensure using `[project.urls]` table format, not inline dict
- **Verification**: Check METADATA file in wheel before uploading

### README Not Rendering
- **Problem**: README displays as plain text on PyPI
- **Solution**: Verify `readme = "README.md"` in `[project]` section
- **Check**: Ensure README is included in `include` list in `[tool.maturin]`

### Version Mismatch
- **Problem**: Python package version doesn't match Rust version
- **Solution**: Verify `requires-python = ">=3.10"` in `pyproject.toml` and sync `pyproject.toml` version before release
- **Prevention**: Follow version sync checklist above

### Missing Wheels for Platform
- **Problem**: Some platform wheels not built
- **Solution**: Check GitHub Actions CI logs for build failures
- **Workaround**: Build locally and upload manually for missing platform

### Import Errors After Install
- **Problem**: `ImportError` or `ModuleNotFoundError` when importing pysof
- **Solution**: Verify wheel includes compiled extension (`_pysof.so` / `_pysof.pyd`)
- **Check**: Inspect wheel contents: `unzip -l dist/pysof-*.whl`

## Release Frequency Recommendations

- **Patch releases** (0.1.X): Bug fixes, documentation updates - as needed
- **Minor releases** (0.X.0): New features, FHIR version support - monthly/quarterly
- **Major releases** (X.0.0): Breaking changes, major refactors - rare

## Version Numbering

Follow Semantic Versioning (SemVer):
- **MAJOR**: Breaking API changes (rare for a 0.x.x project)
- **MINOR**: New features, backwards compatible
- **PATCH**: Bug fixes, documentation, backwards compatible

**Note**: While in 0.x.x, the API is considered unstable and may have breaking changes in minor versions.

## Contacts

- **PyPI Support**: https://pypi.org/help/
- **Maturin Issues**: https://github.com/PyO3/maturin/issues
- **Project Issues**: https://github.com/HeliosSoftware/hfs/issues

## Additional Resources

- [Python Packaging Guide](https://packaging.python.org/)
- [PEP 621 - pyproject.toml specification](https://peps.python.org/pep-0621/)
- [Maturin User Guide](https://www.maturin.rs/)
- [PyPI Markdown Rendering](https://packaging.python.org/guides/making-a-pypi-friendly-readme/)
- [TestPyPI](https://test.pypi.org/)

---

**Last Updated**: 2025-10-30  
**Checklist Version**: 1.0
