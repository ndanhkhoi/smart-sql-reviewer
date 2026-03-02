# SQL Reviewer

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Oracle](https://img.shields.io/badge/Oracle-19c-red.svg)
![Project](https://img.shields.io/badge/Project-HIS%204.0-orange.svg)
![Status](https://img.shields.io/badge/Status-Internal-yellow.svg)

> ⚠️ **Private Project** - Công cụ nội bộ phục vụ dự án HIS 4.0

Công cụ tự động hóa việc thu thập, phân tích và đánh giá hiệu suất các câu truy vấn SQL từ các ứng dụng Java/Spring Boot được giám sát bởi Glowroot APM. Hệ thống sử dụng AI (Z.ai API) để đưa ra các đề xuất tối ưu hóa chi tiết.

## 📋 Mục lục

- [Tổng quan](#-tổng-quan)
- [Kiến trúc hệ thống](#️-kiến-trúc-hệ-thống)
- [Yêu cầu hệ thống](#-yêu-cầu-hệ-thống)
- [Cài đặt](#-cài-đặt)
- [Cấu hình](#️-cấu-hình)
- [Sử dụng](#-sử-dụng)
- [Pipeline xử lý](#-pipeline-xử-lý)
- [Cấu trúc thư mục](#-cấu-trúc-thư-mục)
- [Đầu ra](#-đầu-ra)
- [Troubleshooting](#-troubleshooting)
- [License](#-license)
- [Tác giả](#-tác-giả)
- [Dự án](#-dự-án)

## 🎯 Tổng quan

SQL Reviewer là một công cụ pipeline 6 bước được thiết kế để:

1. **Thu thập SQL** - Tự động lấy các câu truy vấn SQL từ Glowroot APM
2. **Phân tích cú pháp** - Parse SQL để trích xuất thông tin các bảng được sử dụng
3. **Lấy metadata** - Truy vấn metadata từ Oracle Database (cấu trúc bảng, indexes, constraints)
4. **Đánh giá AI** - Sử dụng Z.ai API để phân tích và đề xuất tối ưu hóa
5. **Tạo báo cáo** - Sinh báo cáo HTML trực quan với biểu đồ và thống kê chi tiết
6. **Phân tích độ phức tạp** - Gọi sp_analyze_sql để đếm bảng/cột và xuất báo cáo Excel/Jira

### Tính năng chính

- ✅ Tự động phát hiện các transaction từ Glowroot API
- ✅ Hỗ trợ xử lý song song (parallel processing) cho hiệu suất cao
- ✅ Phân tích SQL theo cú pháp Oracle 19c
- ✅ Đánh giá hiệu suất với các metrics: Performance Score, Complexity Score
- ✅ Phân loại issues theo severity (Critical, High, Medium, Low)
- ✅ Đề xuất tối ưu hóa chi tiết bao gồm gợi ý CREATE INDEX
- ✅ Báo cáo HTML responsive với biểu đồ Chart.js
- ✅ Hỗ trợ tiếng Việt trong đánh giá và báo cáo

## 🏗️ Kiến trúc hệ thống

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          SQL Reviewer Pipeline                           │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐                      │
│  │  Glowroot  │───>│  Step 1:   │───>│ SQL Files  │                      │
│  │    APM     │    │ Fetch SQL  │    │ + SQL Info │                      │
│  └────────────┘    └────────────┘    └────────────┘                      │
│                                            │                             │
│                                            ▼                             │
│                    ┌────────────┐    ┌────────────┐                      │
│                    │  Step 2:   │───>│ Parse Data │                      │
│                    │ Parse SQL  │    │  (Tables)  │                      │
│                    └────────────┘    └────────────┘                      │
│                                            │                             │
│                                            ▼                             │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐                      │
│  │   Oracle   │───>│  Step 3:   │───>│  Metadata  │                      │
│  │  Database  │    │ Query Meta │    │   Files    │                      │
│  └────────────┘    └────────────┘    └────────────┘                      │
│                                            │                             │
│                                            ▼                             │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐                      │
│  │  Z.ai API  │───>│  Step 4:   │───>│ Review Data│                      │
│  │  (AI/LLM)  │    │ Review SQL │    │   Files    │                      │
│  └────────────┘    └────────────┘    └────────────┘                      │
│                                            │                             │
│                                            ▼                             │
│                    ┌────────────┐    ┌────────────┐                      │
│                    │  Step 5:   │───>│ HTML Report│                      │
│                    │ Gen Report │    │            │                      │
│                    └────────────┘    └────────────┘                      │
│                                            │                             │
│                                            ▼                             │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐                      │
│  │   Oracle   │───>│  Step 6:   │───>│Excel+Jira  │                      │
│  │  Database  │    │ Complexity │    │   Reports  │                      │
│  └────────────┘    └────────────┘    └────────────┘                      │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

## 💻 Yêu cầu hệ thống

### Phần mềm

- **Python**: 3.8 trở lên
- **Oracle Client**: Oracle Instant Client hoặc Oracle Database Client
- **Glowroot**: Đang chạy và giám sát các ứng dụng Java

### Thư viện Python chính

| Thư viện | Phiên bản | Mô tả |
|----------|-----------|-------|
| `PyYAML` | 6.0.3 | Đọc file cấu hình YAML |
| `requests` | 2.32.5 | HTTP requests tới Glowroot API |
| `oracledb` | 3.4.1 | Kết nối Oracle Database |
| `sqlglot` | 28.6.0 | SQL Parser |
| `Jinja2` | 3.1.6 | Template engine cho báo cáo HTML |
| `pydantic` | 2.12.5 | Data validation |
| `httpx` | 0.28.1 | HTTP client async |
| `python-dotenv` | 1.2.1 | Load biến môi trường từ .env |
| `openpyxl` | 3.1.5 | Xuất file Excel |

> 📋 Xem đầy đủ dependencies trong file `requirements.txt`

### Dịch vụ bên ngoài

- **Glowroot APM**: Để thu thập SQL queries
- **Oracle Database**: Để lấy metadata schema
- **Z.ai API**: Để đánh giá SQL bằng AI (hoặc API tương thích OpenAI)

## 🚀 Cài đặt

### 1. Clone repository

```bash
git clone <repository-url>
cd sql_reviewer
```

### 2. Tạo môi trường ảo

```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
# hoặc
.\venv\Scripts\activate   # Windows
```

### 3. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### 4. Cấu hình biến môi trường

Tạo file `.env` tại thư mục gốc của project:

```env
# Glowroot Configuration
GLOWROOT_BASE_URL=http://localhost:4000

# Oracle Database Configuration
ORACLE_HOST=your-oracle-host
ORACLE_PORT=1521
ORACLE_SERVICE_NAME=your-service-name
ORACLE_USER=your-username
ORACLE_PASSWORD=your-password

# Z.ai API Configuration
ZAI_API_URL=https://api.z.ai/v1/chat/completions
ZAI_API_KEY=your-api-key
```

## ⚙️ Cấu hình

Cấu hình chính nằm trong file `config/config.yaml`:

### Glowroot Configuration

```yaml
glowroot:
  hours_ago: 48                    # Khoảng thời gian lấy dữ liệu (giờ)
  parallel:
    enabled: true
    max_workers: 10                # Số worker song song
  transaction_discovery:
    initial_limit: 200
    limit_increment: 200
    max_limit: 5000
  agents:                          # Danh sách agents cần giám sát
    - agent_id: "cto-authen-service"
    - agent_id: "cto-billing-service"
    # ...
```

### Oracle Configuration

```yaml
oracle:
  pool:
    min: 10
    max: 20
    increment: 5
  metadata:
    default_schema: "his_data_danquy"
    case_insensitive: true
    parallel:
      enabled: true
      max_workers: 10
```

### Review (AI) Configuration

```yaml
review:
  model: "glm-4.6"                 # Model AI sử dụng
  max_retries: 10                  # Số lần retry khi gọi API
  initial_retry_delay: 2          # Delay ban đầu (giây)
  max_retry_delay: 10             # Delay tối đa (giây)
  max_workers: 3                  # Số API calls song song
```

### Output Configuration

```yaml
output:
  base_dir: "outputs"
  sql_dir: "fetchers/sql"
  sql_info_dir: "fetchers/sql_info"
  parse_dir: "parse"
  metadata_dir: "metadata"
  review_dir: "review"
  logs_dir: "logs"
```

## 📖 Sử dụng

### Tổng quan các Scripts

| Script | Mô tả | Bước |
|--------|-------|------|
| `fetch_sql.py` | Thu thập SQL từ Glowroot APM | Step 1 |
| `parse_sql.py` | Parse SQL để trích xuất tables | Step 2 |
| `query_metadata.py` | Lấy metadata từ Oracle | Step 3 |
| `review_sql.py` | Review SQL bằng AI (Z.ai) | Step 4 |
| `generate_report.py` | Tạo báo cáo HTML | Step 5 |
| `analyze_sql_complexity.py` | Phân tích độ phức tạp SQL + Excel/Jira | Step 6 |
| `clean.py` | Dọn dẹp outputs và logs | Utility |

---

### 📥 Step 1: Fetch SQL (`fetch_sql.py`)

Thu thập SQL queries từ Glowroot APM với auto-discovery transactions.

#### Flags

| Flag | Viết tắt | Mô tả |
|------|----------|-------|
| `--config FILE` | `-c FILE` | Đường dẫn file config (mặc định: `config/config.yaml`) |
| `--list` | `-l` | Liệt kê tất cả agents đã cấu hình |
| `--all` | `-a` | Fetch tất cả agents |
| `--agent AGENT_ID` | | Fetch một agent cụ thể |
| `--hours HOURS` | | Ghi đè `hours_ago` từ config |

#### Ví dụ

```bash
# Interactive mode - chọn agent từ menu
python scripts/fetch_sql.py

# Liệt kê tất cả agents đã cấu hình
python scripts/fetch_sql.py --list
python scripts/fetch_sql.py -l

# Fetch tất cả agents
python scripts/fetch_sql.py --all

# Fetch một agent cụ thể
python scripts/fetch_sql.py --agent cto-billing-service

# Fetch với khoảng thời gian tùy chỉnh (12 giờ gần nhất)
python scripts/fetch_sql.py --all --hours 12

# Sử dụng file config khác
python scripts/fetch_sql.py -c custom_config.yaml --all
```

---

### 🔍 Step 2: Parse SQL (`parse_sql.py`)

Parse các file SQL đã fetch để trích xuất danh sách tables.

#### Flags

Script này sử dụng cấu hình từ `config.yaml`, không có command-line flags bổ sung.

#### Ví dụ

```bash
python scripts/parse_sql.py
```

---

### 🗄️ Step 3: Query Metadata (`query_metadata.py`)

Truy vấn Oracle Database để lấy metadata (columns, indexes, constraints).

#### Flags

Script này sử dụng cấu hình từ `config.yaml`, không có command-line flags bổ sung.

#### Ví dụ

```bash
python scripts/query_metadata.py
```

---

### 🤖 Step 4: Review SQL (`review_sql.py`)

Review SQL bằng AI (Z.ai API) để đánh giá hiệu suất và đề xuất tối ưu.

#### Flags

| Flag | Viết tắt | Mô tả |
|------|----------|-------|
| `--config FILE` | `-c FILE` | Đường dẫn file config (mặc định: `config/config.yaml`) |
| `--clean` | | Xóa thư mục review trước khi chạy |
| `--limit N` | `-n N` | Chỉ review N files đầu tiên (hữu ích cho testing) |
| `--files FILENAME` | | Review file cụ thể theo tên/pattern (có thể dùng nhiều lần) |

#### Ví dụ

```bash
# Review tất cả SQL files
python scripts/review_sql.py

# Xóa kết quả cũ và review lại
python scripts/review_sql.py --clean

# Review 5 files đầu tiên (testing)
python scripts/review_sql.py -n 5
python scripts/review_sql.py --limit 5

# Review file cụ thể theo pattern (case-insensitive)
python scripts/review_sql.py --files query1
python scripts/review_sql.py --files billing

# Review nhiều files cụ thể
python scripts/review_sql.py --files query1 --files report_sql --files login

# Kết hợp nhiều options
python scripts/review_sql.py --clean -n 10 --files billing
```

---

### 📊 Step 5: Generate Report (`generate_report.py`)

Tạo báo cáo HTML từ kết quả review.

#### Arguments (positional)

| Argument | Mô tả | Mặc định |
|----------|-------|----------|
| `outputs_dir` | Thư mục outputs | `outputs/` |
| `output_file` | Đường dẫn file HTML output | `outputs/sql_review_report.html` |
| `template_name` | Tên file template | `report_template.html` |

#### Ví dụ

```bash
# Sử dụng mặc định
python scripts/generate_report.py

# Chỉ định thư mục outputs
python scripts/generate_report.py ./my_outputs

# Chỉ định cả thư mục và file output
python scripts/generate_report.py ./outputs ./reports/my_report.html

# Chỉ định đầy đủ
python scripts/generate_report.py ./outputs ./reports/custom.html custom_template.html
```

---

### 🔬 Step 6: Analyze SQL Complexity (`analyze_sql_complexity.py`)

Phân tích độ phức tạp SQL bằng cách gọi stored procedure `sp_analyze_sql` để đếm số bảng và số cột, sau đó xuất báo cáo Excel và CSV để import vào Jira.

#### Flags

| Flag | Mô tả | Bắt buộc |
|------|-------|----------|
| `--min-cols N` | Filter & highlight đỏ nếu số cột > N | Có |
| `--min-tables N` | Filter & highlight đỏ nếu số bảng > N | Có |
| `--output FILE` | File Excel output (mặc định: `outputs/complexity/sql_complexity_report.xlsx`) | Không |
| `--csv-output FILE` | File CSV output cho Jira (mặc định: `outputs/complexity/jira_subtasks.csv`) | Không |

#### Ví dụ

```bash
# Phân tích query có >10 cột HOẶC >3 bảng
python scripts/analyze_sql_complexity.py --min-cols 10 --min-tables 3

# Phân tích query cực kỳ phức tạp (>20 cột HOẶC >5 bảng)
python scripts/analyze_sql_complexity.py --min-cols 20 --min-tables 5

# Custom output file
python scripts/analyze_sql_complexity.py --min-cols 10 --min-tables 3 \
  --output my_report.xlsx --csv-output my_tasks.csv
```

#### Đầu ra

1. **JSON results**: `outputs/complexity/sql_complexity_results.json`
   - Chứa đầy đủ thông tin phân tích

2. **Excel report**: `outputs/complexity/sql_complexity_report.xlsx`
   - Sheet "Summary": Thống kê số query phức tạp theo agent/transaction
   - Sheet từng agent: Chi tiết các query với highlight đỏ khi vượt ngưỡng

3. **Jira CSV**: `outputs/complexity/jira_subtasks.csv`
   - Format để import subtask vào Jira
   - Columns: `parent,labels,summary,description,assignee,reporter,duedate,estimate`
   - Assignee: `khoinda`, Reporter: `nganntk.tgg`
   - Labels: `TOI_UU_SQL,{agent_name}` (bỏ prefix `cto-`)

---

### 🧹 Utility: Clean (`clean.py`)

Dọn dẹp outputs và logs của project.

#### Flags

| Flag | Mô tả |
|------|-------|
| `--all` | Dọn cả outputs và logs (mặc định nếu không có flag) |
| `--outputs` | Chỉ dọn outputs |
| `--logs` | Chỉ dọn logs |
| `--dry-run` | Xem trước sẽ xóa gì (không thực sự xóa) |

#### Ví dụ

```bash
# Dọn tất cả (outputs + logs)
python scripts/clean.py --all
python scripts/clean.py           # Mặc định cũng là --all

# Chỉ dọn outputs
python scripts/clean.py --outputs

# Chỉ dọn logs
python scripts/clean.py --logs

# Preview trước khi xóa (dry run)
python scripts/clean.py --all --dry-run
python scripts/clean.py --outputs --dry-run
```

---

### 🚀 Chạy toàn bộ pipeline

```bash
# Chạy tuần tự tất cả các bước
python scripts/fetch_sql.py --all && \
python scripts/parse_sql.py && \
python scripts/query_metadata.py && \
python scripts/review_sql.py && \
python scripts/generate_report.py

# Phân tích độ phức tạp SQL (tùy chọn)
python scripts/analyze_sql_complexity.py --min-cols 10 --min-tables 3

# Với clean trước khi review
python scripts/fetch_sql.py --all && \
python scripts/parse_sql.py && \
python scripts/query_metadata.py && \
python scripts/review_sql.py --clean && \
python scripts/generate_report.py

# Dọn sạch và chạy lại từ đầu
python scripts/clean.py --all && \
python scripts/fetch_sql.py --all && \
python scripts/parse_sql.py && \
python scripts/query_metadata.py && \
python scripts/review_sql.py && \
python scripts/generate_report.py
```

## 🔄 Pipeline xử lý

### Step 1: Fetch SQL (`fetch_sql.py`)

- Kết nối tới Glowroot API
- Tự động phát hiện transactions từ các agents đã cấu hình
- Lấy danh sách SQL queries và thông tin execution (duration, count, rows)
- Lưu file `.sql` và `.json` (sql_info)

**Output**: `outputs/fetchers/sql/*.sql`, `outputs/fetchers/sql_info/*.json`

### Step 2: Parse SQL (`parse_sql.py`)

- Đọc các file SQL đã fetch
- Parse bằng thư viện `sqlglot` với dialect Oracle
- Trích xuất danh sách tables được sử dụng (schema.table)

**Output**: `outputs/parse/*.json`

### Step 3: Query Metadata (`query_metadata.py`)

- Đọc danh sách tables từ step 2
- Kết nối Oracle Database
- Lấy metadata: columns, data types, indexes, constraints

**Output**: `outputs/metadata/*.json`

### Step 4: Review SQL (`review_sql.py`)

- Tổng hợp: SQL query + SQL info + Metadata
- Gọi Z.ai API với prompt phân tích chuyên sâu
- Nhận đánh giá: performance score, issues, recommendations

**Output**: `outputs/review/*.json`

### Step 5: Generate Report (`generate_report.py`)

- Tổng hợp dữ liệu từ tất cả các phases
- Sử dụng Jinja2 template engine
- Sinh báo cáo HTML với Chart.js visualizations

**Output**: `outputs/sql_review_report.html`

### Step 6: Analyze SQL Complexity (`analyze_sql_complexity.py`)

- Kết nối Oracle Database
- Gọi stored procedure `sp_analyze_sql` cho mỗi SQL query
- Nhận kết quả: số bảng, số cột, loại operation
- Filter query phức tạp theo ngưỡng (min-cols, min-tables)
- Xuất Excel report với highlight đỏ cho query vượt ngưỡng
- Xuất CSV để import subtask vào Jira

**Output**: `outputs/complexity/sql_complexity_results.json`, `outputs/complexity/sql_complexity_report.xlsx`, `outputs/complexity/jira_subtasks.csv`

## 📁 Cấu trúc thư mục

```
sql_reviewer/
├── config/
│   └── config.yaml              # File cấu hình chính
├── logs/                        # Log files
├── outputs/
│   ├── fetchers/
│   │   ├── sql/                 # SQL queries (.sql files)
│   │   └── sql_info/            # Query execution info (.json)
│   ├── parse/                   # Parsed SQL data (.json)
│   ├── metadata/                # Oracle metadata (.json)
│   ├── review/                  # AI review results (.json)
│   ├── complexity/              # SQL complexity analysis
│   │   ├── sql_complexity_results.json
│   │   ├── sql_complexity_report.xlsx
│   │   └── jira_subtasks.csv
│   └── sql_review_report.html   # Final HTML report
├── resources/
│   ├── metadata_description.json
│   └── review_prompt.txt        # AI system prompt
├── scripts/
│   ├── clean.py                 # Cleanup utility
│   ├── fetch_sql.py             # Step 1: Fetch from Glowroot
│   ├── parse_sql.py             # Step 2: Parse SQL
│   ├── query_metadata.py        # Step 3: Query Oracle metadata
│   ├── review_sql.py            # Step 4: AI review
│   ├── generate_report.py       # Step 5: Generate HTML report
│   └── analyze_sql_complexity.py # Step 6: Analyze complexity + Excel/Jira
├── src/
│   ├── complexity/
│   │   └── oracle_complexity_fetcher.py  # Oracle complexity analyzer
│   ├── fetchers/
│   │   └── glowroot_fetcher.py  # Glowroot API client
│   ├── metadata/
│   │   └── oracle_metadata_fetcher.py  # Oracle metadata client
│   ├── parsers/
│   │   └── sql_parser.py        # SQL parser using sqlglot
│   ├── reviewers/
│   │   └── zai_reviewer.py      # Z.ai API client
│   └── utils/                   # Utility functions
├── templates/
│   └── report_template.html     # Jinja2 HTML template
├── .env                         # Environment variables (không commit)
└── README.md                    # Documentation
```

## 📊 Đầu ra

### HTML Report

Báo cáo HTML bao gồm:

- **Executive Summary**: Tổng quan về số lượng queries, issues, scores
- **Dashboard Charts**: Biểu đồ phân bố severity, categories
- **Query List**: Danh sách tất cả queries với:
  - Performance Score (0-10)
  - Complexity Score (0-10)
  - Execution metrics (duration, count, rows)
  - Issues và recommendations
- **Filter & Search**: Lọc theo agent, severity, category
- **Responsive Design**: Hỗ trợ nhiều kích thước màn hình

### Review JSON Structure

```json
{
  "summary": {
    "performance_score": 7,
    "complexity_score": 5,
    "total_issues": 3,
    "by_severity": {
      "critical": 0,
      "high": 1,
      "medium": 2,
      "low": 0
    },
    "by_category": {
      "performance": 1,
      "nplus1": 0,
      "hibernate": 0,
      "code_quality": 1,
      "index": 1
    },
    "overall_assessment": "Câu query có hiệu suất khá tốt...",
    "priority": "high",
    "effort_to_fix": "medium"
  },
  "issues": [
    {
      "severity": "high",
      "category": "index",
      "title": "Thiếu index cho điều kiện WHERE",
      "description": "Cột X chưa có index...",
      "current_code": "WHERE column_x = :1",
      "recommendation": "CREATE INDEX idx_table_column_x ON table(column_x);",
      "expected_impact": "Giảm 70% thời gian query"
    }
  ]
}
```

## 🔧 Troubleshooting

### Lỗi kết nối Glowroot

```bash
# Kiểm tra Glowroot đang chạy
curl http://localhost:4000/backend/admin/json/agent-ids

# Kiểm tra biến môi trường
echo $GLOWROOT_BASE_URL
```

### Lỗi kết nối Oracle

```bash
# Kiểm tra Oracle client
python -c "import oracledb; print(oracledb.version)"

# Test connection
python -c "
import oracledb
conn = oracledb.connect(
    user='your_user',
    password='your_pass',
    dsn='host:port/service_name'
)
print('Connected!')
conn.close()
"
```

### Lỗi Z.ai API

```bash
# Kiểm tra API key
echo $ZAI_API_KEY

# Test API
curl -X POST $ZAI_API_URL \
  -H "Authorization: Bearer $ZAI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"glm-4.6","messages":[{"role":"user","content":"Hello"}]}'
```

## 📝 License

⚠️ **Private & Proprietary** - Đây là công cụ nội bộ, không được phép phân phối ra bên ngoài.

## 👤 Tác giả

**khoinda** - *Developer*

## 🏥 Dự án

**HIS 4.0** - Hệ thống thông tin bệnh viện thế hệ mới

---

**Developed by khoinda for HIS 4.0 Project** 🏥
