# Đưa lên GitHub và chạy cùng nhóm

Commit cấu hình, Dockerfile, script, notebook code, tài liệu, manifest, fixture nhỏ và bản đồ Graphify. `.gitignore` loại `.env`, dataset staging/spool/archive, output, artifacts lớn và cache. Kiểm tra notebook vì secret nằm trong code vẫn có thể được commit dù output đã xóa.

```powershell
git status --short
git diff
git add README.md AGENTS.md compose.yaml .env.example .gitignore .gitattributes .dockerignore .graphifyignore infrastructure ingestion notebooks tests docs tools graphify-out apps artifacts data outputs
git diff --cached --stat
# Sau khi kiểm tra nội dung staged:
git commit -m "Organize ingestion workspace and document architecture"
```

Remote và push cần URL repository của nhóm. Không đưa `.env` hoặc token vào staged files. Git không mang theo Docker volumes.

Máy bạn cùng nhóm: clone → copy `.env.example` sang `.env` → `docker compose up -d --build` → tải dataset theo nguồn trong manifest → so checksum → publish một lần → chờ HDFS hoàn tất → chạy profile `eda`. Không chạy migration lịch sử trên bản clone mới. Có thể chạy fixture theo `tests/README.md` trước dataset thật.

Registry chứa image/phần mềm, không chứa dataset hoặc volume của máy bạn. Muốn dùng registry thì tag/push image build rồi chỉnh `image:` phù hợp; vẫn cần repo cấu hình và dữ liệu. Dependency trực tiếp được pin version; chưa khóa digest image và toàn bộ dependency bắc cầu nên chưa bảo đảm build byte-identical.
