"""关键流程自测（不依赖 pytest，直接 python -I tests/selftest.py 运行）。

覆盖：
1. CSV 校验：中文表头、重复 (样本,项目)、负数/非数字/空值/缺列/缺批次号
2. 端到端：上传 → 预览(增/改/删/不变分类) → 发布 → 登记版本/行数 → 下游文件哈希一致
3. 并发：两个页面从同一版本发起，仅一个发布成功，另一个 409 须重新预览
4. 崩溃恢复：文件缺失、文件被改坏、遗留临时文件、游离文件、改名后未提交
5. 撤回语义：整批替换后旧行消失；会话取消/重复发布防护
"""
import hashlib
import io
import os
import re
import shutil
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from wq_publish.app import create_app
from wq_publish.storage import LIVE_FILENAME, Storage
from wq_publish.validation import render_csv, validate_csv

PASS, FAIL = 0, 0
EXAMPLE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "examples")


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def read_example(fname):
    with open(os.path.join(EXAMPLE_DIR, fname), "rb") as fh:
        return fh.read()


def upload(client, fname, batch_no="B1"):
    return client.post(
        "/preview",
        data={"batch_no": batch_no,
              "csv_file": (io.BytesIO(read_example(fname)), fname)},
        content_type="multipart/form-data",
    )


# ---------------- 1. 校验单元测试 ----------------
print("== 1. CSV 校验 ==")
rows, errs = validate_csv(read_example("batch_v1.csv"), "B1")
check("合法 CSV 通过并解析出 5 行", errs == [] and len(rows) == 5, str(errs))

rows, errs = validate_csv(read_example("batch_invalid.csv"), "B1")
check("重复(样本,项目)被拒", any("重复" in e for e in errs), str(errs))
check("负浓度被拒", any("负数" in e for e in errs), str(errs))
check("空浓度被拒", any("浓度为空" in e for e in errs), str(errs))
check("空项目被拒", any("检测项目为空" in e for e in errs), str(errs))

bad = "样本号,浓度\nS1,1.0\n".encode()
rows, errs = validate_csv(bad, "B1")
check("缺少检测项目列被拒", rows is None and any("表头" in e for e in errs), str(errs))

rows, errs = validate_csv(read_example("batch_v1.csv"), " ")
check("空批次号被拒", any("批次号" in e for e in errs), str(errs))

for text in ["S1,氨氮,nan", "S1,氨氮,inf", "S1,氨氮,abc"]:
    raw = f"样本号,检测项目,浓度\n{text}\n".encode()
    rows, errs = validate_csv(raw, "B1")
    check(f"非法浓度 {text!r} 被拒", rows is None and len(errs) > 0, str(errs))

raw_zero = "样本号,检测项目,浓度\nS1,氨氮,0\n".encode()
rows, errs = validate_csv(raw_zero, "B1")
check("零浓度（非负边界）通过", errs == [] and rows[0]["concentration"] == 0.0, str(errs))

# ---------------- 2-5. 端到端（隔离临时目录） ----------------
tmp = tempfile.mkdtemp(prefix="wq-selftest-")
data_dir = os.path.join(tmp, "data")
down_dir = os.path.join(tmp, "downstream")
app = create_app(data_dir=data_dir, downstream_dir=down_dir)
client = app.test_client()

print("== 2. 首次发布 v1 ==")
resp = upload(client, "batch_v1.csv", "BATCH-001")
check("预览页 200 且显示增删改", resp.status_code == 200
      and "新增".encode() in resp.data and "撤回".encode() in resp.data)
import re
m = re.search(rb'/publish/([0-9a-f]+)', resp.data)
check("预览页含发布令牌", m is not None)
token1 = m.group(1).decode()

resp = client.post(f"/publish/{token1}")
check("确认发布后 302 回首页", resp.status_code == 302)
reg = app.storage.current_registry()
check("登记为 v1、5 行", reg["current_version"] == 1 and reg["row_count"] == 5,
      f"got {dict(reg) if reg else None}")
live = os.path.join(down_dir, LIVE_FILENAME)
check("下游 CSV 已生成", os.path.exists(live))
file_hash = hashlib.sha256(open(live, "rb").read()).hexdigest()
check("文件哈希与登记一致", file_hash == reg["content_hash"])
check("下游目录无临时文件残留",
      not [n for n in os.listdir(down_dir) if n.startswith(".tmp-")])
resp = client.get("/published.csv")
check("已发布 CSV 可下载", resp.status_code == 200 and b"S001" in resp.data)
resp = client.post(f"/publish/{token1}")
check("同一会话不可重复发布（返回 409 过期页）", resp.status_code == 409
      and app.storage.get_session(token1)["status"] == "published")

print("== 3. 修订预览的增/改/删/不变分类 ==")
resp = upload(client, "batch_v2_revision.csv", "BATCH-002")
body = resp.data.decode()
# v2 相对 v1：S004氨氮新增；S001氨氮、S002COD 修改；S002氨氮 撤回；S001总磷/S003总磷 不变
check("新增 1 行", "+1" in body and "S004" in body)
check("修改 2 行", "~2" in body and "1.25" in body and "35.0" in body)
check("撤回 1 行（S002 氨氮）", "−1" in body and "S002" in body)
check("不变 2 行", "不变项 2" in body)
token2 = re.search(rb'/publish/([0-9a-f]+)', resp.data).group(1).decode()
client.post(f"/publish/{token2}")
reg = app.storage.current_registry()
check("发布后登记为 v2、5 行", reg["current_version"] == 2 and reg["row_count"] == 5)
current = app.storage.current_rows()
check("撤回生效：S002 氨氮已不存在",
      not any(r["sample_no"] == "S002" and r["analyte"] == "氨氮" for r in current))
check("修改生效：S001 氨氮=1.3",
      any(r["sample_no"] == "S001" and r["analyte"] == "氨氮"
          and r["concentration"] == 1.3 for r in current))
file_hash = hashlib.sha256(open(live, "rb").read()).hexdigest()
check("v2 文件哈希与登记一致", file_hash == reg["content_hash"])

print("== 4. 同版本并发修订：只允许一个成功 ==")
r_a = upload(client, "batch_v1.csv", "CONCUR-A")
r_b = upload(client, "batch_v2_revision.csv", "CONCUR-B")
tok_a = re.search(rb'/publish/([0-9a-f]+)', r_a.data).group(1).decode()
tok_b = re.search(rb'/publish/([0-9a-f]+)', r_b.data).group(1).decode()
ok = client.post(f"/publish/{tok_a}")
check("A 发布成功 → v3", ok.status_code == 302
      and app.storage.current_registry()["current_version"] == 3)
conflict = client.post(f"/publish/{tok_b}")
check("B 发布被拒并返回 409 过期预览页", conflict.status_code == 409
      and "该预览已过期".encode() in conflict.data)
sess_b = app.storage.get_session(tok_b)
check("B 会话被标记 obsolete", sess_b["status"] == "obsolete")
reg_before = app.storage.current_registry()["current_version"]
retry = upload(client, "batch_v2_revision.csv", "CONCUR-B-RETRY")
tok_b2 = re.search(rb'/publish/([0-9a-f]+)', retry.data).group(1).decode()
client.post(f"/publish/{tok_b2}")
check("B 重新预览后可发布 → v4",
      app.storage.current_registry()["current_version"] == reg_before + 1)
check("A 的发布未被覆盖：v4 文件行数=5",
      app.storage.current_registry()["row_count"] == 5)

print("== 5. 崩溃后重启自动恢复 ==")
# 5a 下游文件被删除
os.unlink(live)
app2 = create_app(data_dir=data_dir, downstream_dir=down_dir)
reg = app2.storage.current_registry()
check("5a 文件缺失后重建且哈希与登记一致",
      os.path.exists(live)
      and hashlib.sha256(open(live, "rb").read()).hexdigest() == reg["content_hash"])

# 5b 文件被外部改坏
with open(live, "ab") as fh:
    fh.write(b"CORRUPTED\n")
app3 = create_app(data_dir=data_dir, downstream_dir=down_dir)
reg = app3.storage.current_registry()
check("5b 损坏文件被按登记重写",
      hashlib.sha256(open(live, "rb").read()).hexdigest() == reg["content_hash"])

# 5c 遗留半写临时文件
tmp_orphan = os.path.join(down_dir, ".tmp-publish-deadbeef.csv")
open(tmp_orphan, "wb").write(b"partial")
create_app(data_dir=data_dir, downstream_dir=down_dir)
check("5c 遗留临时文件被清理", not os.path.exists(tmp_orphan))

# 5d 下游目录出现未登记游离文件
stranger = os.path.join(down_dir, "stranger.csv")
open(stranger, "wb").write(b"x")
create_app(data_dir=data_dir, downstream_dir=down_dir)
check("5d 游离文件被移入 quarantine",
      not os.path.exists(stranger)
      and any(f.endswith("stranger.csv")
              for f in os.listdir(os.path.join(data_dir, "quarantine"))))

# 5e 模拟“文件已改名但事务未提交”：文件内容比登记新，重启应回滚到登记版本
with app3.storage._connect() as conn:
    old_csv = app3.storage._render_version(conn, reg["current_version"])
newer = render_csv([{"sample_no": "ZX", "analyte": "氨氮", "concentration": 9.9}])
app3.storage._atomic_write(newer)
check("预置：文件已被换成未登记的新版本",
      open(live, "rb").read().decode() != old_csv)
app4 = create_app(data_dir=data_dir, downstream_dir=down_dir)
check("5e 改名后未提交 → 恢复为登记的完整旧版",
      open(live, "rb").read().decode() == old_csv)
events = app4.storage.recent_events(50)
check("恢复动作写入事件日志", any(e["kind"] == "recover" for e in events))

print("== 6. 会话取消 ==")
r = upload(client, "batch_v1.csv", "CANCEL-ME")
tok = re.search(rb'/publish/([0-9a-f]+)', r.data).group(1).decode()
v_before = app.storage.current_registry()["current_version"]
client.post(f"/cancel/{tok}")
check("取消后发布被拒绝（409 且会话 cancelled）",
      client.post(f"/publish/{tok}").status_code == 409
      and app.storage.get_session(tok)["status"] == "cancelled")
check("取消不影响当前版本",
      app.storage.current_registry()["current_version"] == v_before)

# ---------------- 7. 空间不足导致发布失败：供数一致性 ----------------

def read_live():
    with open(live, "rb") as fh:
        return fh.read()


def trigger_db_full(storage, sql="results"):
    """用触发器稳定模拟 SQLITE_FULL（真实写满磁盘在测试中不可控）。"""
    conn = sqlite3.connect(storage.db_path)
    conn.execute(
        f"CREATE TRIGGER wq_test_full BEFORE INSERT ON {sql} "
        "BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END"
    )
    conn.commit()
    conn.close()


def drop_trigger(storage):
    conn = sqlite3.connect(storage.db_path)
    conn.execute("DROP TRIGGER IF EXISTS wq_test_full")
    conn.commit()
    conn.close()


def flash_text(resp):
    return re.findall(r'<div class="flash [^"]+">(.*?)</div>', resp.data.decode(), re.S)


print("== 7. 数据库空间不足：已有批次失败 → 恢复原登记 CSV ==")
base_version = app.storage.current_registry()["current_version"]
old_live = read_live()
old_hash = app.storage.current_registry()["content_hash"]
r = upload(client, "batch_v2_revision.csv", "FULL-REV")
tok_full = re.search(rb'/publish/([0-9a-f]+)', r.data).group(1).decode()
trigger_db_full(app.storage)
resp = client.post(f"/publish/{tok_full}")
drop_trigger(app.storage)
check("失败响应为 302 回首页（页面可展示错误）", resp.status_code == 302)
page = client.get("/")
messages = flash_text(page)
check("页面准确提示发布失败", any("发布失败" in m for m in messages), str(messages))
check("页面未提示发布成功", not any("发布成功" in m for m in messages))
check("登记版本未变", app.storage.current_registry()["current_version"] == base_version)
check("登记哈希未变", app.storage.current_registry()["content_hash"] == old_hash)
check("下游 CSV 已恢复为完整原登记内容", read_live() == old_live)
check("下游 CSV 哈希与登记一致",
      hashlib.sha256(read_live()).hexdigest() == old_hash)
check("没有残留临时文件/备份链接",
      not [n for n in os.listdir(down_dir) if n.startswith(".tmp-")
           or n.startswith(".prev-")])
check("会话保持 pending，可直接重新提交",
      app.storage.get_session(tok_full)["status"] == "pending")
check("失败事件写入日志",
      any(e["kind"] == "publish-failed" for e in app.storage.recent_events(20)))
retry = client.post(f"/publish/{tok_full}")
check("空间恢复后重新提交成功且只登记一次",
      retry.status_code == 302
      and app.storage.current_registry()["current_version"] == base_version + 1)
check("重新提交后下游 CSV 与登记一致",
      hashlib.sha256(read_live()).hexdigest()
      == app.storage.current_registry()["content_hash"])

print("== 8. 失败发生在换文件之后：返回前恢复旧版 ==")
base_version = app.storage.current_registry()["current_version"]
old_live = read_live()
r = upload(client, "batch_v1.csv", "FULL-AFTER-WRITE")
tok_after = re.search(rb'/publish/([0-9a-f]+)', r.data).group(1).decode()
real_atomic = type(app.storage)._atomic_write.__get__(app.storage)

def write_then_fail(text):
    real_atomic(text)
    raise sqlite3.OperationalError("database or disk is full")

app.storage._atomic_write = write_then_fail
try:
    client.post(f"/publish/{tok_after}")
finally:
    app.storage._atomic_write = real_atomic
check("换文件后失败：下游在响应前恢复完整旧版", read_live() == old_live)
check("换文件后失败：登记未变且会话仍 pending",
      app.storage.current_registry()["current_version"] == base_version
      and app.storage.get_session(tok_after)["status"] == "pending")
check("换文件后失败：无残留备份链接",
      not [n for n in os.listdir(down_dir) if n.startswith(".prev-")])
client.post(f"/publish/{tok_after}")
check("恢复后重新提交成功（不重复登记）",
      app.storage.current_registry()["current_version"] == base_version + 1)

print("== 9. 首次发布失败：不得留下可读取的正式 CSV ==")
tmp_first = tempfile.mkdtemp(prefix="wq-first-")
first = create_app(data_dir=os.path.join(tmp_first, "data"),
                   downstream_dir=os.path.join(tmp_first, "downstream"))
c_first = first.test_client()
res = c_first.post(
    "/preview",
    data={"batch_no": "FIRST", "csv_file": (io.BytesIO(read_example("batch_v1.csv")),
                                           "batch_v1.csv")},
    content_type="multipart/form-data",
)
tok_first = re.search(rb'/publish/([0-9a-f]+)', res.data).group(1).decode()
trigger_db_full(first.storage)
resp = c_first.post(f"/publish/{tok_first}")
drop_trigger(first.storage)
first_live = os.path.join(tmp_first, "downstream", LIVE_FILENAME)
check("首次失败：页面提示发布失败",
      any("发布失败" in m for m in flash_text(c_first.get("/"))))
check("首次失败：登记仍为空", first.storage.current_registry() is None)
check("首次失败：下游无可读取正式 CSV", not os.path.exists(first_live))
check("首次失败：/published.csv 返回 404",
      c_first.get("/published.csv").status_code == 404)
check("首次失败：无残留临时文件",
      not [n for n in os.listdir(os.path.dirname(first_live)) if n.startswith(".")])
retry = c_first.post(f"/publish/{tok_first}")
check("首次失败后重新提交成功登记为 v1",
      retry.status_code == 302
      and first.storage.current_registry()["current_version"] == 1
      and os.path.exists(first_live))
shutil.rmtree(tmp_first, ignore_errors=True)

print("== 10. 失败恢复与并发发布交错：不撤销后来成功的版本 ==")
tmp_race = tempfile.mkdtemp(prefix="wq-race-")
race = create_app(data_dir=os.path.join(tmp_race, "data"),
                  downstream_dir=os.path.join(tmp_race, "downstream"))
c_race = race.test_client()
res = c_race.post(
    "/preview",
    data={"batch_no": "RACE-V1", "csv_file": (io.BytesIO(read_example("batch_v1.csv")),
                                             "batch_v1.csv")},
    content_type="multipart/form-data",
)
c_race.post(f"/publish/{re.search(rb'/publish/([0-9a-f]+)', res.data).group(1).decode()}")
res = c_race.post(
    "/preview",
    data={"batch_no": "RACE-OURS",
          "csv_file": (io.BytesIO(read_example("batch_v2_revision.csv")),
                       "batch_v2_revision.csv")},
    content_type="multipart/form-data",
)
tok_ours = re.search(rb'/publish/([0-9a-f]+)', res.data).group(1).decode()
other_rows = validate_csv(read_example("batch_v1.csv"), "RACE-OTHER")[0]
tok_other = race.storage.create_session("RACE-OTHER", 1, other_rows)
original_recover = race.storage._recover_after_failure
real_atomic_race = type(race.storage)._atomic_write.__get__(race.storage)

def other_publishes_first(**kwargs):
    # 另一服务进程在我们失败与恢复之间成功发布了 v2
    Storage(race.storage.data_dir, race.storage.downstream_dir).publish(tok_other)
    return original_recover(**kwargs)

race.storage._atomic_write = write_then_fail
race.storage._recover_after_failure = other_publishes_first
try:
    c_race.post(f"/publish/{tok_ours}")
finally:
    race.storage._atomic_write = real_atomic_race
    race.storage._recover_after_failure = original_recover
reg = race.storage.current_registry()
race_live = os.path.join(tmp_race, "downstream", LIVE_FILENAME)
check("交错：登记保留其他进程成功的 v2", reg["current_version"] == 2)
check("交错：下游文件仍是他人的 v2 完整内容",
      hashlib.sha256(open(race_live, "rb").read()).hexdigest() == reg["content_hash"])
check("交错：页面提示他的版本已保留，未撤销",
      any("已保留" in m or "未撤销" in m for m in flash_text(c_race.get("/"))))
shutil.rmtree(tmp_race, ignore_errors=True)

shutil.rmtree(tmp, ignore_errors=True)
print(f"\n结果：{PASS} 通过，{FAIL} 失败")
sys.exit(1 if FAIL else 0)
