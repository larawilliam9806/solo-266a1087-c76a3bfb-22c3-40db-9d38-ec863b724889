"""Flask 路由：上传校验 → 预览增删改 → 确认整批替换。"""
from __future__ import annotations

import hashlib
import os
import secrets
import sqlite3

from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from .storage import LIVE_FILENAME, PublishConflictError, PublishFailureError, Storage
from .validation import format_concentration, validate_csv

MAX_UPLOAD_BYTES = 16 * 1024 * 1024


def create_app(data_dir: str | None = None, downstream_dir: str | None = None) -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES

    base = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    data_dir = data_dir or os.environ.get("WQ_DATA_DIR") or os.path.join(base, "data")
    downstream_dir = (
        downstream_dir
        or os.environ.get("WQ_DOWNSTREAM_DIR")
        or os.path.join(base, "downstream")
    )

    secret_path = os.path.join(data_dir, "flask_secret")
    os.makedirs(data_dir, exist_ok=True)
    if os.path.exists(secret_path):
        with open(secret_path, "rb") as fh:
            app.secret_key = fh.read()
    else:
        key = secrets.token_bytes(32)
        with open(secret_path, "wb") as fh:
            fh.write(key)
        os.chmod(secret_path, 0o600)
        app.secret_key = key

    storage = Storage(data_dir, downstream_dir)
    app.storage = storage  # 供自测脚本直接访问

    @app.template_filter("conc")
    def _conc(value):
        return format_concentration(value)

    @app.get("/")
    def index():
        reg = storage.current_registry()
        rows = storage.current_rows()
        events = storage.recent_events()
        return render_template(
            "index.html", reg=reg, rows=rows, events=events,
            live_filename=LIVE_FILENAME,
        )

    @app.post("/preview")
    def preview():
        upload = request.files.get("csv_file")
        batch_no = request.form.get("batch_no", "")
        if upload is None or not upload.filename:
            flash("请选择仪器导出的 CSV 文件", "error")
            return redirect(url_for("index"))
        raw = upload.read()
        if len(raw) > MAX_UPLOAD_BYTES:
            flash("CSV 文件超过 16MB 上限", "error")
            return redirect(url_for("index"))

        rows, errors = validate_csv(raw, batch_no)
        if errors:
            for msg in errors:
                flash(msg, "error")
            return redirect(url_for("index"))

        reg = storage.current_registry()
        base_version = reg["current_version"] if reg else 0
        token = storage.create_session(batch_no.strip(), base_version, rows)
        diff = storage.preview_diff(rows)
        storage.log(
            "preview",
            f"会话 {token[:8]} 基于 v{base_version} 预览批次 {batch_no.strip()}："
            f"新增 {len(diff['inserts'])} / 修改 {len(diff['updates'])} / "
            f"撤回 {len(diff['deletes'])} / 不变 {len(diff['unchanged'])}",
        )
        return render_template(
            "preview.html",
            token=token,
            batch_no=batch_no.strip(),
            base_version=base_version,
            current_version=base_version,
            rows=rows,
            diff=diff,
            live_filename=LIVE_FILENAME,
        )

    @app.post("/publish/<token>")
    def publish(token):
        session = storage.get_session(token)
        if session is None:
            flash("修订会话不存在或已过期，请重新上传 CSV", "error")
            return redirect(url_for("index"))

        reg = storage.current_registry()
        current_version = reg["current_version"] if reg else 0
        if session["status"] != "pending" or session["base_version"] != current_version:
            # 会话在预览期间被别的发布超越：作废会话，展示基于旧版的差异，要求重新预览
            if session["status"] == "pending":
                storage.mark_obsolete(token, current_version)
                stale_reason = (
                    f"您基于 v{session['base_version']} 发起修订，"
                    f"但当前版本已变为 v{current_version}（另一页面已成功发布）"
                )
            else:
                reason_map = {
                    "published": "该修订已经发布，不能重复发布",
                    "obsolete": "该预览已被更新的发布超越而作废",
                    "cancelled": "该修订已被放弃（取消）",
                }
                stale_reason = reason_map.get(session["status"], "该修订会话已失效")
            diff = storage.preview_diff(session["rows"])
            storage.log(
                "conflict",
                f"会话 {token[:8]} 发布被拒（基线 v{session['base_version']}，"
                f"当前 v{current_version}，状态 {session['status']}）",
            )
            return render_template(
                "preview.html",
                token=token,
                batch_no=session["batch_no"],
                base_version=session["base_version"],
                current_version=current_version,
                rows=session["rows"],
                diff=diff,
                stale=True,
                stale_reason=stale_reason,
                live_filename=LIVE_FILENAME,
            ), 409

        try:
            new_version, row_count = storage.publish(token)
        except PublishConflictError as exc:
            flash(str(exc), "error")
            return redirect(url_for("index"))
        except PublishFailureError as exc:
            # 发布失败：登记未生效，下游 CSV 已在返回前按登记恢复（首次发布则移除）。
            # 会话保持 pending，数据库/磁盘空间恢复后可直接重新提交，仍按当前版本预览。
            app.logger.warning("publish failed: %s", exc)
            if exc.not_started:
                flash(
                    f"当前有其他发布正在写入或数据库暂不可用（{exc}），"
                    "请稍后重新提交发布，本次未产生任何数据变更",
                    "error",
                )
                return redirect(url_for("index"))
            reg = storage.current_registry()
            current_version = reg["current_version"] if reg else 0
            live = os.path.join(storage.downstream_dir, LIVE_FILENAME)
            live_present = os.path.exists(live)
            live_matches = False
            if live_present:
                with open(live, "rb") as fh:
                    live_matches = (
                        hashlib.sha256(fh.read()).hexdigest() == reg["content_hash"]
                    ) if reg else False
            flash(f"发布失败：{exc}。本次修订未登记，当前版本仍为 v{current_version}。",
                  "error")
            if exc.newer_version:
                flash(
                    f"期间其他页面已成功发布 v{exc.newer_version}，其版本已保留，"
                    "未撤销。请基于当前版本重新预览后再发布。",
                    "info",
                )
            elif not live_present:
                flash(
                    "首次发布失败，下游目录未留下可读取的正式 CSV。"
                    "空间恢复后可直接重新提交预览并发布。",
                    "info",
                )
            elif live_matches:
                flash(
                    f"下游 CSV 已恢复为登记 v{current_version} 的完整内容，"
                    "下游不会读到未登记的数据。空间恢复后可直接重新提交发布。",
                    "info",
                )
            else:
                flash(
                    "自动恢复未完成（可能空间仍然不足），请先释放数据库/磁盘空间；"
                    "系统会在下次启动时继续按登记核对，期间下游可能读到不一致内容。",
                    "error",
                )
            if not exc.repair_ok and live_present and live_matches and not exc.newer_version:
                # 文件已实际恢复，但恢复事务未能提交（空间仍紧张）：提示核对结果
                flash(
                    f"下游 CSV 已按登记 v{current_version} 恢复完整，"
                    "但恢复记录未能写入数据库；不影响下游读取。",
                    "info",
                )
            return redirect(url_for("index"))
        except sqlite3.OperationalError as exc:  # 极端并发下写锁等待超时
            app.logger.warning("publish lock contention: %s", exc)
            flash("当前有其他发布正在写入，请稍后重新预览再发布", "error")
            return redirect(url_for("index"))
        flash(f"发布成功：v{new_version} 已生效，共 {row_count} 行", "success")
        return redirect(url_for("index"))

    @app.post("/cancel/<token>")
    def cancel(token):
        if storage.cancel_session(token):
            flash("已放弃本次修订，当前发布版本未受影响", "info")
        return redirect(url_for("index"))

    @app.get("/published.csv")
    def published_csv():
        if not os.path.exists(storage.live_path):
            abort(404, description="尚无已发布的 CSV")
        return send_file(
            storage.live_path, mimetype="text/csv",
            download_name=LIVE_FILENAME, as_attachment=False,
        )

    @app.errorhandler(413)
    def too_large(_):
        flash("上传文件超过 16MB 上限", "error")
        return redirect(url_for("index"))

    return app
