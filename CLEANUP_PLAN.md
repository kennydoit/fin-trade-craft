# Codebase Cleanup Plan

## Files/Directories to Remove

### High Priority (Remove Immediately)
```bash
# One-off verification scripts (development artifacts)
rm -rf archive/checks_and_verifications/
rm -rf archive/copilot_test_scripts/
rm -rf archive/debug_and_demo/
rm -rf archive/tests/

# Legacy/superseded code
rm -rf archive/legacy_extractors/
rm archive/cleanup_economic_indicators.py
rm archive/extract_historical_options.py
rm archive/restore_database.py
rm archive/transform_base.py

# Duplicate/outdated scripts
rm scripts/debug_api_failures.py
rm scripts/final_verification_summary.py
rm scripts/find_good_symbols.py  # Duplicate of archive version
```

### Medium Priority (Review then Remove)
```bash
# Most prompt/markdown files (keep 1-2 key ones)
# Review prompts/ directory - keep POSTGRES_MIGRATION_COMPLETE.md
# Remove implementation outlines and daily update notes

# Migration files (only if no longer needed for rollback)
# Review archive/migrations/ - keep critical migration scripts
# Remove temporary/development migration files
```

### Low Priority (Keep for Now)
```bash
# Keep these for reference/troubleshooting:
archive/sql/                    # Reusable SQL patterns
archive/organize_schemas.py     # Schema management utility
scripts/init_source_schema.py   # Active schema initialization
```

## Expected Impact
- **~200+ files removed** (~70% reduction in non-core files)
- **Faster IDE performance** and project navigation
- **Clearer project structure** for new developers
- **Reduced maintenance overhead**

## Safety Notes
- All files are in Git history (can be recovered)
- Remove in batches to test impact
- Keep one backup branch before major cleanup
- Document any critical scripts before removal

## Timeline
1. **Week 1**: Remove high priority files
2. **Week 2**: Review and clean medium priority 
3. **Week 3**: Final cleanup and documentation update
