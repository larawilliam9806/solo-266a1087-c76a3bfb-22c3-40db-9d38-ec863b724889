"""SQLite 登记 + 下游目录原子发布 + 启动一致性恢复。

设计要点
--------
* SQLite 是版本/行数登记的唯一真相源；下游目录中的 CSV 只是它的镜像。
* 发布在单个 ``BEGIN IMMEDIATE`` 事务内完成：先在事务里写入新版本结果与登记
  （尚未提交），再写同目录临时文件、fsync 并 ``os.replace`` 原子改名，最后提交。
  数据库空间不足等写入错误会在触碰下游文件之前发生；即使错误发生在换文件之后，
  失败路径也会在返回错误响应前按登记恢复出完整 CSV（首次发布则移除未登记文件）。
* 失败恢复与正常发布使用同一把 SQLite 写锁；若在失败与恢复之间其他进程已经
  成功发布了更新版本，则绝不回退其文件。
* 提交后进程若在改名完成前崩溃：重启时按数据库登记重新生成 CSV，
  并清理遗留临时文件；登记里不存在的游离文件会被移入 quarantine 隔离。
"""
from __future__ import annotations

import errno
import hashlib
import json
import os
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone

from .validation import format_concentration, render_csv

LIVE_FILENAME = "published_results.csv"
SQLITE_FULL = 13


class PublishConflictError(Exception):
    """确认发布时基线版本已过期，需要重新预览。"""


class PublishFailureError(Exception):
    """发布写入失败；失败路径已完成登记一致性核对与下游文件恢复。"""

    def __init__(
        self,
        message: str,
        *,
        restored_version: int | None = None,
        newer_version: int | None = None,
        repair_ok: bool = True,
        not_started: bool = False,
    ):
        super().__init__(message)
        self.restored_version = restored_version
        self.newer_version = newer_version
        self.repair_ok = repair_ok
        self.not_started = not_started


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _is_out_of_space(exc: BaseException) -> bool:
    if isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
        return True
    if isinstance(exc, sqlite3.Error):
        if getattr(exc, "sqlite_errorcode", None) == SQLITE_FULL:
            return True
        text = str(exc).lower()
        return "disk is full" in text or "database is full" in text
    return False


def _failure_reason(exc: BaseException) -> str:
    if _is_out_of_space(exc):
        return "数据库或磁盘空间不足"
    if isinstance(exc, sqlite3.Error):
        return f"数据库写入失败（{exc}）"
    if isinstance(exc, OSError):
        return f"下游文件写入失败（{exc}）"
    return f"存储写入失败（{exc}）"


class Storage:
    def __init__(self, data_dir: str, downstream_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        self.downstream_dir = os.path.abspath(downstream_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.downstream_dir, exist_ok=True)
        self.db_path = os.path.join(self.data_dir, "wq_publish.db")
        self.live_path = os.path.join(self.downstream_dir, LIVE_FILENAME)
        self.quarantine_dir = os.path.join(self.data_dir, "quarantine")
        os.makedirs(self.quarantine_dir, exist_ok=True)
        self._init_db()
        self.recover()

    # ---------- 基础 ----------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS registry (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_version INTEGER NOT NULL,
                    batch_no TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    published_at TEXT NOT NULL,
                    content_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version INTEGER NOT NULL,
                    batch_no TEXT NOT NULL,
                    sample_no TEXT NOT NULL,
                    analyte TEXT NOT NULL,
                    concentration REAL NOT NULL CHECK (concentration >= 0),
                    UNIQUE (version, sample_no, analyte)
                );
                CREATE INDEX IF NOT EXISTS idx_results_version
                    ON results(version);
                CREATE TABLE IF NOT EXISTS revision_sessions (
                    token TEXT PRIMARY KEY,
                    batch_no TEXT NOT NULL,
                    base_version INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending','published','obsolete','cancelled'))
                );
                CREATE TABLE IF NOT EXISTS event_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL
                );
                """
            )

    def _log(self, conn: sqlite3.Connection, kind: str, detail: str) -> None:
        conn.execute(
            "INSERT INTO event_log(ts, kind, detail) VALUES (?,?,?)",
            (_now(), kind, detail),
        )

    def log(self, kind: str, detail: str) -> None:
        with self._connect() as conn:
            self._log(conn, kind, detail)

    # ---------- 启动恢复 ----------
    def recover(self) -> list[str]:
        """重启后核对下游文件与登记的一致性，返回本次恢复动作说明。"""
        actions: list[str] = []
        with self._connect() as conn:
            reg = conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()

            # 1) 清理崩溃遗留的临时文件/备份链接（可能只写了一半，绝不能被下游读到）
            for name in os.listdir(self.downstream_dir):
                if name.startswith(".tmp-publish-") or name.startswith(".prev-publish-"):
                    path = os.path.join(self.downstream_dir, name)
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
                    msg = f"清理遗留临时文件 downstream/{name}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)

            # 2) 登记为空：正式文件（若存在）属于无登记游离文件，隔离并保留
            if reg is None:
                if os.path.exists(self.live_path):
                    dest = self._quarantine(LIVE_FILENAME)
                    msg = f"数据库无任何发布登记，将下游游离文件移入 {os.path.basename(dest)}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)
                return actions

            expected_csv = self._render_version(conn, reg["current_version"])
            expected_hash = hashlib.sha256(expected_csv.encode("utf-8")).hexdigest()

            # 3) 文件缺失/哈希不符：以登记为准原子重建
            need_rewrite = True
            if os.path.exists(self.live_path):
                with open(self.live_path, "rb") as fh:
                    actual_hash = hashlib.sha256(fh.read()).hexdigest()
                need_rewrite = actual_hash != expected_hash

            if need_rewrite:
                self._atomic_write(expected_csv)
                msg = (
                    f"下游 CSV 与登记 v{reg['current_version']} 不一致，"
                    "已按数据库登记重建（恢复为完整新版本）"
                )
                actions.append(msg)
                self._log(conn, "recover", msg)
            else:
                self._log(
                    conn,
                    "recover",
                    f"启动核对通过：下游 CSV 与登记 v{reg['current_version']} 一致",
                )

            # 4) 同目录下游文件白名单：非登记文件一律隔离
            for name in os.listdir(self.downstream_dir):
                full = os.path.join(self.downstream_dir, name)
                if os.path.isfile(full) and name != LIVE_FILENAME:
                    dest = self._quarantine(name)
                    msg = f"下游目录存在未登记文件 {name}，已移入 {os.path.basename(dest)}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)

            # 5) 超过 24 小时仍未确认的预览会话作废，避免无限期堆积
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(
                timespec="seconds"
            )
            stale = conn.execute(
                "SELECT token FROM revision_sessions "
                "WHERE status='pending' AND created_at < ?",
                (cutoff,),
            ).fetchall()
            for s in stale:
                conn.execute(
                    "UPDATE revision_sessions SET status='obsolete' WHERE token=?",
                    (s["token"],),
                )
                self._log(
                    conn, "recover",
                    f"预览会话 {s['token'][:8]} 超过 24 小时未确认，已作废",
                )
        return actions

    def _quarantine(self, name: str) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        dest = os.path.join(self.quarantine_dir, f"{stamp}-{name}")
        os.replace(os.path.join(self.downstream_dir, name), dest)
        return dest

    def _atomic_write(self, csv_text: str) -> None:
        """同目录临时文件 + fsync + 原子改名，保证下游只看到完整文件。"""
        tmp_path = os.path.join(
            self.downstream_dir, f".tmp-publish-{uuid.uuid4().hex}.csv"
        )
        fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
                fh.write(csv_text)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.live_path)
            self._fsync_dir(self.downstream_dir)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _remove_live(self) -> None:
        """删除未登记的正式文件（首次发布失败时使用），并 fsync 目录。"""
        try:
            os.unlink(self.live_path)
        except FileNotFoundError:
            pass
        self._fsync_dir(self.downstream_dir)

    def _backup_live(self) -> str | None:
        """为当前正式文件建立同目录硬链接备份（不额外占用数据块）。

        用于「文件已换成本次新版本、但事务提交失败」时无需再写新内容即可原子恢复。
        """
        if not os.path.exists(self.live_path):
            return None
        backup = os.path.join(
            self.downstream_dir, f".prev-publish-{uuid.uuid4().hex}.csv"
        )
        try:
            os.link(self.live_path, backup)
        except OSError:
            return None
        return backup

    @staticmethod
    def _unlink_quiet(path: str | None) -> None:
        if not path:
            return
        try:
            os.unlink(path)
        except OSError:
            pass

    def _hash_file(self, path: str) -> str | None:
        try:
            with open(path, "rb") as fh:
                return _sha256(fh.read())
        except OSError:
            return None

    def _restore_live(
        self,
        conn: sqlite3.Connection,
        *,
        attempted_version: int | None,
        attempted_hash: str | None,
        file_written: bool,
        backup_path: str | None = None,
    ) -> dict:
        """把下游正式文件恢复成登记内容（只在发布失败路径调用）。

        调用方持有 ``BEGIN IMMEDIATE``：其他发布进程的「换文件 + 提交」与本恢复
        串行，因此这里读到的登记与文件属于同一一致快照。
        返回 {"ok", "restored": 恢复到的登记版本或 None, "newer": 比本次尝试更新的
        已登记版本或 None}。
        """
        reg = conn.execute("SELECT * FROM registry WHERE id=1").fetchone()
        live_hash = self._hash_file(self.live_path)

        if reg is None:
            # 首次发布失败：绝不留下可读取的未登记正式 CSV
            if file_written and live_hash is not None and live_hash == attempted_hash:
                try:
                    self._remove_live()
                except OSError:
                    return {"ok": False, "restored": None, "newer": None}
            self._unlink_quiet(backup_path)
            return {"ok": True, "restored": None, "newer": None}

        version = reg["current_version"]
        newer = (
            version
            if attempted_version is not None and version > attempted_version
            else None
        )
        if live_hash == reg["content_hash"]:
            self._unlink_quiet(backup_path)
            return {"ok": True, "restored": None, "newer": newer}

        try:
            # 优先用发布前的硬链接备份原样恢复（磁盘仍满时也能完成，且不撤销
            # 其他进程的登记——仅当备份内容就是当前登记内容时才使用）。
            if (
                backup_path
                and self._hash_file(backup_path) == reg["content_hash"]
                and version in self._known_versions(conn)
            ):
                os.replace(backup_path, self.live_path)
                self._fsync_dir(self.downstream_dir)
            else:
                if version not in self._known_versions(conn):
                    return {"ok": False, "restored": version, "newer": newer}
                csv_text = self._render_version(conn, version)
                if _sha256(csv_text.encode("utf-8")) != reg["content_hash"]:
                    return {"ok": False, "restored": version, "newer": newer}
                self._atomic_write(csv_text)
        except OSError:
            return {"ok": False, "restored": version, "newer": newer}
        self._unlink_quiet(backup_path)
        if self._hash_file(self.live_path) != reg["content_hash"]:
            return {"ok": False, "restored": version, "newer": newer}
        return {"ok": True, "restored": version, "newer": newer}

    @staticmethod
    def _known_versions(conn: sqlite3.Connection) -> set[int]:
        return {row[0] for row in conn.execute("SELECT DISTINCT version FROM results")}

    def _recover_after_failure(
        self,
        *,
        attempted_version: int | None = None,
        attempted_hash: str | None = None,
        file_written: bool = False,
        backup_path: str | None = None,
    ) -> dict:
        """发布失败后立即恢复（不依赖重启），与发布共用同一把 SQLite 写锁。

        用 BEGIN IMMEDIATE 读取「当前登记 + 正式文件」的一致快照：若等待锁期间
        另一进程已成功发布，这里看到的就是新登记与它写好的新文件，不会误删/回退。
        空间不足时事件日志也可能写不进去：恢复动作本身优先，日志失败不阻断恢复。
        """
        info: dict = {"ok": False, "restored": None, "newer": None}
        for attempt in range(1, 4):
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                result = self._restore_live(
                    conn,
                    attempted_version=attempted_version,
                    attempted_hash=attempted_hash,
                    file_written=file_written,
                    backup_path=backup_path,
                )
                conn.commit()
                info = result
            except sqlite3.Error:
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass
                time.sleep(0.05 * attempt)
            finally:
                conn.close()
            if info["ok"]:
                break

        # 事件日志尽力而为：空间不足时写不进去也不能影响恢复结论
        try:
            if info["ok"] and info["restored"]:
                self.log(
                    "recover",
                    f"发布失败后即时恢复：下游 CSV 已恢复为登记 "
                    f"v{info['restored']} 的完整内容",
                )
            elif info["ok"] and info["newer"]:
                self.log(
                    "recover",
                    f"发布失败后即时核对：其他发布已写入 v{info['newer']}，"
                    "保留其版本，不撤销",
                )
        except sqlite3.Error:
            pass
        return info

    @staticmethod
    def _fsync_dir(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    # ---------- 读 ----------
    def current_registry(self):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()

    def current_rows(self) -> list[dict]:
        with self._connect() as conn:
            reg = conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()
            if reg is None:
                return []
            return self._version_rows(conn, reg["current_version"])

    def _version_rows(self, conn: sqlite3.Connection, version: int) -> list[dict]:
        cur = conn.execute(
            "SELECT sample_no, analyte, concentration FROM results "
            "WHERE version = ? ORDER BY sample_no, analyte",
            (version,),
        )
        return [dict(r) for r in cur.fetchall()]

    def _render_version(self, conn: sqlite3.Connection, version: int) -> str:
        return render_csv(self._version_rows(conn, version))

    def recent_events(self, limit: int = 20):
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM event_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

    def get_session(self, token: str):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM revision_sessions WHERE token = ?", (token,)
            ).fetchone()
            if row is None:
                return None
            data = dict(row)
            data["rows"] = json.loads(data["payload"])
            return data

    def create_session(self, batch_no: str, base_version: int, rows: list[dict]) -> str:
        token = uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO revision_sessions(token, batch_no, base_version, payload,"
                " created_at, status) VALUES (?,?,?,?,?,?)",
                (
                    token,
                    batch_no,
                    base_version,
                    json.dumps(rows, ensure_ascii=False),
                    _now(),
                    "pending",
                ),
            )
        return token

    def mark_obsolete(self, token: str, current_version: int) -> None:
        """预览在确认前已被其他发布超越：作废该会话，强制重新预览。"""
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE revision_sessions SET status='obsolete' "
                "WHERE token=? AND status='pending'",
                (token,),
            )
            if cur.rowcount:
                self._log(
                    conn, "conflict",
                    f"会话 {token[:8]} 已过期（当前 v{current_version}），"
                    "拒绝覆盖，需重新预览",
                )

    def cancel_session(self, token: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE revision_sessions SET status='cancelled' "
                "WHERE token=? AND status='pending'",
                (token,),
            )
            if cur.rowcount:
                self._log(conn, "cancel", f"放弃修订会话 {token[:8]}")
                return cur.rowcount > 0
        return False

    # ---------- 预览差异 ----------
    def preview_diff(self, new_rows: list[dict]) -> dict:
        """与当前已发布版本比对，返回 增/改/删/不变 四类差异。"""
        old = {(r["sample_no"], r["analyte"]): r["concentration"]
               for r in self.current_rows()}
        new = {(r["sample_no"], r["analyte"]): r["concentration"] for r in new_rows}
        inserts, updates, deletes, unchanged = [], [], [], []
        for key, val in new.items():
            if key not in old:
                inserts.append({"sample_no": key[0], "analyte": key[1], "new": val})
            elif old[key] != val:
                updates.append(
                    {"sample_no": key[0], "analyte": key[1],
                     "old": old[key], "new": val}
                )
            else:
                unchanged.append({"sample_no": key[0], "analyte": key[1], "value": val})
        for key, val in old.items():
            if key not in new:
                deletes.append({"sample_no": key[0], "analyte": key[1], "old": val})
        inserts.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        updates.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        deletes.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        unchanged.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        return {
            "inserts": inserts, "updates": updates,
            "deletes": deletes, "unchanged": unchanged,
        }

    # ---------- 确认发布（核心事务） ----------
    def publish(self, token: str) -> tuple[int, int]:
        """确认整批替换。返回 (新版本号, 行数)。

        并发控制：BEGIN IMMEDIATE 立即拿写锁，事务内再次核对会话基线版本，
        若期间已有别人发布（current_version > base_version），本会话作废，
        抛出 PublishConflictError，调用方须重新预览。

        失败安全：新版本结果与登记先写入未提交事务，再原子换下游文件，最后提交。
        * 数据库在换文件前失败（如空间不足）：事务回滚，下游文件从未被触碰；
        * 换文件后提交失败或文件写入失败：回滚事务，并在返回错误前立即按登记
          恢复（首次则删除未登记文件），不依赖重启；
        * 恢复阶段若发现其他进程已发布更新版本，则保留其文件，绝不回退。
        失败统一抛出 PublishFailureError，其 attributes 记录了恢复结果。
        """
        conn = self._connect()
        file_touched = False
        began = False
        backup_path: str | None = None
        attempted_version: int | None = None
        attempted_hash: str | None = None
        failure: tuple[BaseException, bool] | None = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            began = True
            session = conn.execute(
                "SELECT * FROM revision_sessions WHERE token=?", (token,)
            ).fetchone()
            if session is None:
                raise PublishConflictError("修订会话不存在或已失效，请重新上传预览")
            if session["status"] != "pending":
                label = {"published": "已发布", "obsolete": "已过期",
                         "cancelled": "已取消"}.get(
                    session["status"], session["status"])
                raise PublishConflictError(f"该修订会话{label}，请重新上传预览")

            reg = conn.execute("SELECT * FROM registry WHERE id=1").fetchone()
            current_version = reg["current_version"] if reg else 0
            if session["base_version"] != current_version:
                conn.execute(
                    "UPDATE revision_sessions SET status='obsolete' WHERE token=?",
                    (token,),
                )
                self._log(
                    conn, "conflict",
                    f"会话 {token[:8]} 基于 v{session['base_version']} 发起，"
                    f"当前已为 v{current_version}，拒绝覆盖，需重新预览",
                )
                raise PublishConflictError(
                    f"版本已变化：您基于 v{session['base_version']} 预览，"
                    f"当前已是 v{current_version}，请重新预览后再发布"
                )

            rows = json.loads(session["payload"])
            new_version = current_version + 1
            csv_text = render_csv(rows)
            content_hash = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()

            attempted_version = current_version
            attempted_hash = content_hash

            # 1) 先落库（未提交）：空间不足等错误在触碰下游文件前即暴露
            conn.executemany(
                "INSERT INTO results(version, batch_no, sample_no, analyte,"
                " concentration) VALUES (?,?,?,?,?)",
                [
                    (new_version, session["batch_no"], r["sample_no"],
                     r["analyte"], r["concentration"])
                    for r in rows
                ],
            )
            conn.execute(
                "INSERT INTO registry(id, current_version, batch_no, row_count,"
                " published_at, content_hash) VALUES (1,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "current_version=excluded.current_version,"
                " batch_no=excluded.batch_no, row_count=excluded.row_count,"
                " published_at=excluded.published_at,"
                " content_hash=excluded.content_hash",
                (new_version, session["batch_no"], len(rows), _now(),
                 content_hash),
            )
            conn.execute(
                "UPDATE revision_sessions SET status='published' WHERE token=?",
                (token,),
            )
            self._log(
                conn, "publish",
                f"发布 v{new_version}：批次 {session['batch_no']}，{len(rows)} 行，"
                f"sha256={content_hash[:12]}…",
            )

            # 2) 备份旧正式文件（硬链接，不占额外空间），再原子换文件
            backup_path = self._backup_live()
            file_touched = True
            self._atomic_write(csv_text)
            # 3) 提交。提交失败（如 WAL 空间不足）时登记不生效，需恢复文件
            conn.commit()
            self._unlink_quiet(backup_path)
            return new_version, len(rows)
        except PublishConflictError:
            raise
        except (sqlite3.Error, OSError) as exc:
            # 交由 finally 释放连接/写锁后，再做失败恢复（同一写锁串行）
            failure = (exc, file_touched)
        finally:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            # 先释放连接与写锁，恢复流程再以独立事务与并发发布串行
            try:
                conn.close()
            except sqlite3.Error:
                pass

        if failure is None:  # pragma: no cover - 正常路径已在上面 return
            raise AssertionError("publish() 未返回且无异常")
        exc, file_touched = failure
        # 开始事务前就失败（连接/等待写锁超时）：未触碰下游文件，无需恢复；
        # 调用方按原有「稍后重试」语义提示，会话仍为 pending 可重新提交。
        if not began and not file_touched:
            self._unlink_quiet(backup_path)
            try:
                self.log("publish-failed",
                         f"会话 {token[:8]} 发布未开始：{_failure_reason(exc)}")
            except sqlite3.Error:
                pass
            raise PublishFailureError(
                _failure_reason(exc), repair_ok=True, not_started=True
            ) from exc
        repair = self._recover_after_failure(
            attempted_version=attempted_version,
            attempted_hash=attempted_hash,
            file_written=file_touched,
            backup_path=backup_path,
        )
        try:
            self.log(
                "publish-failed",
                f"会话 {token[:8]} 发布失败：{_failure_reason(exc)}；"
                f"恢复{'成功' if repair['ok'] else '未完成'}"
                + (
                    f"，下游已恢复为登记 v{repair['restored']}"
                    if repair["restored"] else ""
                )
                + (
                    f"，检测到更新的发布 v{repair['newer']}，保留其文件"
                    if repair["newer"] else ""
                ),
            )
        except sqlite3.Error:
            pass  # 空间不足时日志可能也写不进去，不能影响恢复结果返回
        raise PublishFailureError(
            _failure_reason(exc),
            restored_version=repair["restored"],
            newer_version=repair["newer"],
            repair_ok=repair["ok"],
        ) from exc

    # ---------- 页面辅助 ----------
    @staticmethod
    def display_concentration(value: float) -> str:
        return format_concentration(value)
