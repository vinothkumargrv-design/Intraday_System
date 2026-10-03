"""
Notification system for the Rapid Fire Intraday Trading System.

Sends alerts to Telegram and/or email based on ``notifications`` in
config.yaml. Each channel has a ``notify_on`` list of event names:

    TRADE_ENTRY, TRADE_EXIT, STOP_LOSS_HIT, TAKE_PROFIT_HIT,
    KILL_SWITCH_ACTIVATED, DAILY_LIMIT_HIT, SYSTEM_START, SYSTEM_STOP,
    ERROR, DAILY_REPORT

Notifications are queued and delivered by a background thread so trading
threads never block on network I/O. Telegram is rate limited per minute.
The notifier plugs into the execution engine and risk manager through their
``subscribe`` callbacks (see ``on_execution_event`` / ``on_risk_event``).
"""

from __future__ import annotations

import json
import queue
import smtplib
import ssl
import threading
import time
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.text import MIMEText
from typing import Any, Callable, Dict, List, Optional

from .config_loader import parse_time
from .logger import get_logger
from .models import NotificationLevel

Config = Dict[str, Any]

LEVEL_RANK = {
    NotificationLevel.INFO: 0,
    NotificationLevel.TRADE: 1,
    NotificationLevel.WARNING: 2,
    NotificationLevel.ERROR: 3,
    NotificationLevel.CRITICAL: 4,
}

LEVEL_ICON = {
    NotificationLevel.INFO: "ℹ️",
    NotificationLevel.TRADE: "💹",
    NotificationLevel.WARNING: "⚠️",
    NotificationLevel.ERROR: "❌",
    NotificationLevel.CRITICAL: "🚨",
}


@dataclass
class Notification:
    """A single notification."""
    event: str
    title: str
    message: str
    level: NotificationLevel = NotificationLevel.INFO
    timestamp: datetime = field(default_factory=datetime.now)
    data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {"event": self.event, "level": self.level.value, "title": self.title,
                "message": self.message, "timestamp": self.timestamp.isoformat(), "data": self.data}


class Notifier:
    """Routes notifications to Telegram and email."""

    def __init__(self, config: Config,
                 report_provider: Optional[Callable[[], str]] = None):
        self.logger = get_logger("notifier")
        cfg = (config or {}).get('notifications', {}) or {}
        self.enabled = bool(cfg.get('enabled', True))

        tg = cfg.get('telegram', {}) or {}
        self.telegram_enabled = self.enabled and bool(tg.get('enabled')) and bool(tg.get('bot_token'))
        self.telegram_token = tg.get('bot_token', "")
        self.telegram_chat_id = tg.get('chat_id', "")
        self.telegram_events = set(tg.get('notify_on', []) or [])
        self.telegram_rate = int((tg.get('rate_limit', {}) or {}).get('max_messages_per_minute', 10))
        self._telegram_sent: deque = deque()

        em = cfg.get('email', {}) or {}
        self.email_enabled = self.enabled and bool(em.get('enabled')) and bool(em.get('to_emails'))
        self.email_cfg = em
        self.email_events = set(em.get('notify_on', []) or [])
        report = em.get('daily_report', {}) or {}
        self.daily_report_enabled = self.email_enabled and bool(report.get('enabled', False))
        self.daily_report_time = parse_time(report.get('send_time', '18:00'))
        self._report_sent_on: Optional[Any] = None
        self.report_provider = report_provider

        # Extra in-process listeners (tests, dashboards)
        self.callbacks: List[Callable[[Notification], None]] = []
        self.history: deque = deque(maxlen=200)

        self._queue: "queue.Queue[Optional[Notification]]" = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._running = False

        self.logger.info(f"Notifier initialized (telegram={'on' if self.telegram_enabled else 'off'}, "
                         f"email={'on' if self.email_enabled else 'off'})")

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._worker, name="Notifier", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        """Flush queued notifications and stop."""
        if not self._running:
            return
        self._queue.put(None)
        if self._thread:
            self._thread.join(timeout=timeout)
        self._running = False
        self._thread = None

    def _worker(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=30)
            except queue.Empty:
                self._maybe_send_daily_report()
                continue
            if item is None:
                break
            self._deliver(item)
            self._maybe_send_daily_report()

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def notify(self, event: str, title: str, message: str,
               level: NotificationLevel = NotificationLevel.INFO,
               data: Optional[Dict[str, Any]] = None) -> Notification:
        """Queue a notification (delivered synchronously if the worker is not running)."""
        notification = Notification(event=event, title=title, message=message, level=level,
                                    data=data or {})
        self.history.append(notification)
        if self._running:
            self._queue.put(notification)
        else:
            self._deliver(notification)
        return notification

    def on_execution_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """Subscriber for ExecutionEngine events."""
        if event_type == 'entry_filled':
            self.notify("TRADE_ENTRY", f"Entered {data.get('symbol')}",
                        f"Filled {data.get('quantity'):g} @ {data.get('price', 0):.2f}",
                        NotificationLevel.TRADE, data)
        elif event_type == 'position_closed':
            reason = (data.get('tags') or 'EXIT').split(',')[0]
            event = {"STOP_LOSS": "STOP_LOSS_HIT", "TAKE_PROFIT": "TAKE_PROFIT_HIT"}.get(reason, "TRADE_EXIT")
            self.notify(event, f"Closed {data.get('symbol')} ({reason})",
                        f"{data.get('side')} {data.get('quantity'):g}: {data.get('entry_price', 0):.2f} -> "
                        f"{data.get('exit_price', 0):.2f}, net PnL {data.get('net_pnl', 0):.2f}",
                        NotificationLevel.TRADE, data)

    def on_risk_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """Subscriber for RiskManager events."""
        if event_type == 'kill_switch_activated':
            self.notify("KILL_SWITCH_ACTIVATED", "KILL SWITCH ACTIVATED",
                        f"Trading halted: {data.get('reason', 'unknown')}",
                        NotificationLevel.CRITICAL, data)
            if 'loss limit' in str(data.get('reason', '')).lower():
                self.notify("DAILY_LIMIT_HIT", "Daily loss limit hit", str(data.get('reason')),
                            NotificationLevel.CRITICAL, data)

    def register_callback(self, callback: Callable[[Notification], None]) -> None:
        self.callbacks.append(callback)

    # =========================================================================
    # DELIVERY
    # =========================================================================

    def _deliver(self, notification: Notification) -> None:
        for callback in list(self.callbacks):
            try:
                callback(notification)
            except Exception as e:
                self.logger.error(f"Notification callback failed: {e}")

        if self.telegram_enabled and notification.event in self.telegram_events:
            self._send_telegram(notification)
        if self.email_enabled and notification.event in self.email_events:
            self._send_email(f"[RapidFire] {notification.title}", self._format_text(notification))

    def _format_text(self, n: Notification) -> str:
        return f"{LEVEL_ICON.get(n.level, '')} {n.title}\n{n.message}\n{n.timestamp:%Y-%m-%d %H:%M:%S}"

    def _send_telegram(self, notification: Notification) -> None:
        now = time.monotonic()
        while self._telegram_sent and now - self._telegram_sent[0] > 60:
            self._telegram_sent.popleft()
        if len(self._telegram_sent) >= self.telegram_rate and notification.level not in (
                NotificationLevel.CRITICAL, NotificationLevel.ERROR):
            self.logger.warning(f"Telegram rate limit reached; dropped: {notification.title}")
            return
        self._telegram_sent.append(now)

        payload = json.dumps({"chat_id": self.telegram_chat_id,
                              "text": self._format_text(notification),
                              "disable_web_page_preview": True}).encode()
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{self.telegram_token}/sendMessage",
            data=payload, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                result = json.loads(response.read().decode() or "{}")
                if not result.get("ok"):
                    self.logger.error(f"Telegram API error: {result.get('description')}")
        except Exception as e:
            self.logger.error(f"Telegram send failed: {e}")

    def _send_email(self, subject: str, body: str) -> None:
        cfg = self.email_cfg
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"] = cfg.get("from_email") or cfg.get("username", "")
        msg["To"] = ", ".join(cfg.get("to_emails", []))
        try:
            with smtplib.SMTP(cfg.get("smtp_host", "smtp.gmail.com"), int(cfg.get("smtp_port", 587)),
                              timeout=15) as server:
                server.starttls(context=ssl.create_default_context())
                if cfg.get("username"):
                    server.login(cfg["username"], cfg.get("password", ""))
                server.sendmail(msg["From"], cfg.get("to_emails", []), msg.as_string())
        except Exception as e:
            self.logger.error(f"Email send failed: {e}")

    def _maybe_send_daily_report(self) -> None:
        if not (self.daily_report_enabled and self.report_provider):
            return
        now = datetime.now()
        if now.time() >= self.daily_report_time and self._report_sent_on != now.date():
            self._report_sent_on = now.date()
            try:
                self._send_email(f"[RapidFire] Daily report {now:%Y-%m-%d}", self.report_provider())
            except Exception as e:
                self.logger.error(f"Daily report failed: {e}")


# =============================================================================
# SINGLETON
# =============================================================================

_global_notifier: Optional[Notifier] = None


def get_notifier(config: Optional[Config] = None) -> Notifier:
    """Get (or create) the process-wide notifier."""
    global _global_notifier
    if _global_notifier is None:
        if config is None:
            from .config_loader import get_config_loader
            config = get_config_loader().load()
        _global_notifier = Notifier(config)
    return _global_notifier
