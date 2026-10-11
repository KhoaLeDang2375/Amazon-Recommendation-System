# Repository navigation

- Read `docs/project-map.md` first. Use `graphify-out/GRAPH_REPORT.md` and Graphify queries with a small budget to locate relevant files. Verify source before changes; generated graphs can be stale.
- Default real dataset: `/project/raw/video_games_metadata/v1`. Smoke data belongs under `/project/testing`, never in real dataset directories.
- Preserve `hadoop-demo` identity and named volumes. Never format HDFS or remove volumes to fix layout issues.
- Drain Flume before changing routing. Keep channel/checkpoint state and do not republish existing batches casually.
- Do not scan datasets, artifacts, `.env`, archive backups, or caches for repository understanding. They can be large or private.
- Preserve user notebook code; back up locally before clearing outputs. Keep tokens and outputs out of Git.
- `apps/web`, `artifacts/analytics`, and `artifacts/models` are placeholders. Do not add web/model implementations without a user request.
- User documentation is Vietnamese. Distinguish host PowerShell, container shell, and notebook Python commands.
- Refresh the graph after structural changes. Commit generated navigation artifacts, not caches.
