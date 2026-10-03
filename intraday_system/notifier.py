"""
Notification module for the Rapid Fire Intraday Trading System.

This module provides:
- Telegram notifications
- Email alerts
- System status notifications
- Trade alerts
- Error notifications
- Configurable notification channels and thresholds
"""

from __future__ import annotations

import asyncio
import smtplib
import ssl
from dataclasses import dataclass
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
import threading

import aiohttp

from .models import (
    AggregatedSignal,
    NotificationLevel,
    Order,
    OrderSide,
    OrderStatus,
    Position,
    PortfolioState,
    RiskState,
    SignalType,
    SystemState,
    Tick,
    Trade,
)

from .logger import get_logger
from .config_loader import Config
from .models import LogLevel


class NotificationType(Enum):
    """Types of notifications."""
    TRADE_OPEN = "TRADE_OPEN"
    TRADE_CLOSE = "TRADE_CLOSE"
    TRADE_SL = "TRADE_SL"
    TRADE_TP = "TRADE_TP"
    ORDER_PLACED = "ORDER_PLACED"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    ORDER_REJECTED = "ORDER_REJECTED"
    SIGNAL_GENERATED = "SIGNAL_GENERATED"
    RISK_ALERT = "RISK_ALERT"
    KILL_SWITCH = "KILL_SWITCH"
    SYSTEM_START = "SYSTEM_START"
    SYSTEM_STOP = "SYSTEM_STOP"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    DAILY_SUMMARY = "DAILY_SUMMARY"
    WEEKLY_SUMMARY = "WEEKLY_SUMMARY"
    PORTFOLIO_UPDATE = "PORTFOLIO_UPDATE"


@dataclass
class Notification:
    """Notification message structure."""
    notification_type: NotificationType
    level: NotificationLevel
    title: str
    message: str
    timestamp: datetime = datetime.now()
    data: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.data is None:
            self.data = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert notification to dictionary."""
        return {
            "type": self.notification_type.value,
            "level": self.level.value,
            "title": self.title,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data
        }


@dataclass
class NotificationConfig:
    """Configuration for notification channels."""
    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    
    email_enabled: bool = False
    email_smtp_server: str = "smtp.gmail.com"
    email_smtp_port: int = 587
    email_username: str = ""
    email_password: str = ""
    email_from: str = ""
    email_to: List[str] = None
    
    min_level: NotificationLevel = NotificationLevel.INFO
    
    # Notification thresholds
    trade_pnl_threshold: float = 0.0  # Notify on trades with PnL > this
    daily_pnl_threshold: float = 0.0  # Notify on daily PnL changes > this
    drawdown_threshold: float = 1.0  # Notify on drawdown > this percent
    
    def __post_init__(self):
        if self.email_to is None:
            self.email_to = []


class Notifier:
    """
    Central notification manager.
    
    Handles all notifications to various channels (Telegram, Email, etc.)
    with configurable thresholds and formatting.
    """
    
    def __init__(self, config: Config):
        """
        Initialize the notifier.
        
        Args:
            config: System configuration
        """
        self.logger = get_logger("notifier")
        self.config = config
        
        # Extract notification config
        self.notification_config = self._extract_notification_config(config)
        
        # Notification queue
        self._notification_queue: asyncio.Queue[Notification] = asyncio.Queue()
        
        # Callbacks for different notification types
        self._callbacks: Dict[NotificationType, List[Callable]] = {}
        
        # Lock for thread safety
        self._lock = threading.Lock()
        
        # Running state
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
        
        self.logger.info("Notifier initialized")
    
    def _extract_notification_config(self, config: Config) -> NotificationConfig:
        """
        Extract notification configuration from main config.
        """
        try:
            # Try to get notification settings from config
            notif_config = getattr(config, 'notifications', {})
            
            telegram_enabled = notif_config.get('telegram_enabled', False)
            telegram_bot_token = notif_config.get('telegram_bot_token', "")
            telegram_chat_id = notif_config.get('telegram_chat_id', "")
            
            email_enabled = notif_config.get('email_enabled', False)
            email_smtp_server = notif_config.get('email_smtp_server', "smtp.gmail.com")
            email_smtp_port = notif_config.get('email_smtp_port', 587)
            email_username = notif_config.get('email_username', "")
            email_password = notif_config.get('email_password', "")
            email_from = notif_config.get('email_from', "")
            email_to = notif_config.get('email_to', [])
            
            min_level_str = notif_config.get('min_level', "INFO")
            min_level = NotificationLevel(min_level_str)
            
            return NotificationConfig(
                telegram_enabled=telegram_enabled,
                telegram_bot_token=telegram_bot_token,
                telegram_chat_id=telegram_chat_id,
                email_enabled=email_enabled,
                email_smtp_server=email_smtp_server,
                email_smtp_port=email_smtp_port,
                email_username=email_username,
                email_password=email_password,
                email_from=email_from,
                email_to=email_to,
                min_level=min_level,
                trade_pnl_threshold=notif_config.get('trade_pnl_threshold', 0.0),
                daily_pnl_threshold=notif_config.get('daily_pnl_threshold', 0.0),
                drawdown_threshold=notif_config.get('drawdown_threshold', 1.0)
            )
        except Exception as e:
            self.logger.error(f"Error extracting notification config: {e}")
            return NotificationConfig()
    
    def start(self) -> None:
        """
        Start the notification processor.
        """
        if self._running:
            return
        
        self._running = True
        self._processor_task = asyncio.create_task(self._process_notifications())
        self.logger.info("Notifier started")
    
    def stop(self) -> None:
        """
        Stop the notification processor.
        """
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
        self.logger.info("Notifier stopped")
    
    async def _process_notifications(self) -> None:
        """
        Process notifications from the queue.
        """
        self.logger.info("Notification processor started")
        
        while self._running:
            try:
                # Wait for notification with timeout
                try:
                    notification = await asyncio.wait_for(
                        self._notification_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Check if notification level meets minimum
                if notification.level.value < self.notification_config.min_level.value:
                    self._notification_queue.task_done()
                    continue
                
                # Send to all enabled channels
                if self.notification_config.telegram_enabled:
                    await self._send_telegram_notification(notification)
                
                if self.notification_config.email_enabled:
                    await self._send_email_notification(notification)
                
                # Trigger callbacks
                await self._trigger_callbacks(notification)
                
                self._notification_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"Error processing notification: {e}")
        
        self.logger.info("Notification processor stopped")
    
    async def send_notification(self, notification: Notification) -> None:
        """
        Send a notification.
        
        Args:
            notification: Notification to send
        """
        await self._notification_queue.put(notification)
    
    async def _send_telegram_notification(self, notification: Notification) -> None:
        """
        Send notification via Telegram.
        """
        try:
            # Format message
            message = self._format_telegram_message(notification)
            
            # Send via Telegram API
            url = f"https://api.telegram.org/bot{self.notification_config.telegram_bot_token}/sendMessage"
            
            payload = {
                "chat_id": self.notification_config.telegram_chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get('ok'):
                            self.logger.debug(f"Telegram notification sent: {notification.title}")
                        else:
                            self.logger.error(f"Telegram API error: {result.get('description')}")
                    else:
                        self.logger.error(f"Telegram HTTP error: {response.status}")
        
        except Exception as e:
            self.logger.error(f"Error sending Telegram notification: {e}")
    
    async def _send_email_notification(self, notification: Notification) -> None:
        """
        Send notification via email.
        """
        try:
            # Format email
            subject = self._format_email_subject(notification)
            body = self._format_email_body(notification)
            
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = self.notification_config.email_from
            msg['To'] = ", ".join(self.notification_config.email_to)
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'html'))
            
            # Send email
            context = ssl.create_default_context()
            
            with smtplib.SMTP(
                self.notification_config.email_smtp_server,
                self.notification_config.email_smtp_port
            ) as server:
                server.starttls(context=context)
                server.login(
                    self.notification_config.email_username,
                    self.notification_config.email_password
                )
                server.sendmail(
                    self.notification_config.email_from,
                    self.notification_config.email_to,
                    msg.as_string()
                )
            
            self.logger.debug(f"Email notification sent: {notification.title}")
        
        except Exception as e:
            self.logger.error(f"Error sending email notification: {e}")
    
    def _format_telegram_message(self, notification: Notification) -> str:
        """
        Format notification for Telegram.
        """
        emoji_map = {
            NotificationLevel.INFO: "ℹ️",
            NotificationLevel.WARNING: "⚠️",
            NotificationLevel.ERROR: "❌",
            NotificationLevel.CRITICAL: "🚨",
            NotificationLevel.TRADE: "📈",
            NotificationType.TRADE_OPEN: "🟢",
            NotificationType.TRADE_CLOSE: "🔴",
            NotificationType.TRADE_SL: "🛑",
            NotificationType.TRADE_TP: "🎯",
            NotificationType.ORDER_PLACED: "📝",
            NotificationType.ORDER_FILLED: "✅",
            NotificationType.ORDER_CANCELLED: "❌",
            NotificationType.ORDER_REJECTED: "🚫",
            NotificationType.SIGNAL_GENERATED: "🔮",
            NotificationType.RISK_ALERT: "⚠️",
            NotificationType.KILL_SWITCH: "💀",
            NotificationType.SYSTEM_START: "🚀",
            NotificationType.SYSTEM_STOP: "🛑",
            NotificationType.SYSTEM_ERROR: "💥",
            SignalType.BUY: "🟢 BUY",
            SignalType.SELL: "🔴 SELL",
            SignalType.HOLD: "⚪ HOLD",
            OrderSide.BUY: "🟢 BUY",
            OrderSide.SELL: "🔴 SELL"
        }
        
        # Get emoji for level
        level_emoji = emoji_map.get(notification.level, "")
        
        # Get emoji for type
        type_emoji = emoji_map.get(notification.notification_type, "")
        
        # Format message
        message = f"{level_emoji} {type_emoji} <b>{notification.title}</b>\n\n{notification.message}"
        
        # Add timestamp
        timestamp_str = notification.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        message += f"\n\n<i>{timestamp_str}</i>"
        
        return message
    
    def _format_email_subject(self, notification: Notification) -> str:
        """
        Format email subject.
        """
        return f"[RapidFire] {notification.level.name}: {notification.title}"
    
    def _format_email_body(self, notification: Notification) -> str:
        """
        Format email body.
        """
        color_map = {
            NotificationLevel.INFO: "#2196F3",
            NotificationLevel.WARNING: "#FF9800", 
            NotificationLevel.ERROR: "#F44336",
            NotificationLevel.CRITICAL: "#9C27B0",
            NotificationLevel.TRADE: "#4CAF50"
        }
        
        color = color_map.get(notification.level, "#607D8B")
        
        html = f"""
        <html>
        <body>
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
                <div style="background-color: {color}; color: white; padding: 10px; text-align: center; border-radius: 5px;">
                    <h2 style="margin: 0;">{notification.title}</h2>
                </div>
                <div style="margin-top: 20px; line-height: 1.6;">
                    <p><strong>Type:</strong> {notification.notification_type.value}</p>
                    <p><strong>Level:</strong> {notification.level.value}</p>
                    <p><strong>Message:</strong></p>
                    <p style="background-color: #f5f5f5; padding: 10px; border-left: 4px solid {color};">
                        {notification.message}
                    </p>
        """
        
        # Add data if available
        if notification.data:
            html += "<p><strong>Details:</strong></p><ul>"
            for key, value in notification.data.items():
                html += f"<li><strong>{key}:</strong> {value}</li>"
            html += "</ul>"
        
        html += f"""
                    <p style="color: #888; font-size: 12px; margin-top: 20px;">
                        {notification.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
                    </p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    async def _trigger_callbacks(self, notification: Notification) -> None:
        """
        Trigger registered callbacks for this notification type.
        """
        if notification.notification_type not in self._callbacks:
            return
        
        for callback in self._callbacks[notification.notification_type]:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(notification)
                else:
                    callback(notification)
            except Exception as e:
                self.logger.error(f"Error in notification callback: {e}")
    
    def register_callback(
        self, 
        notification_type: NotificationType, 
        callback: Callable[[Notification], None]
    ) -> None:
        """
        Register a callback for a specific notification type.
        
        Args:
            notification_type: Type of notification
            callback: Function to call when notification is received
        """
        with self._lock:
            if notification_type not in self._callbacks:
                self._callbacks[notification_type] = []
            self._callbacks[notification_type].append(callback)
    
    def unregister_callback(
        self, 
        notification_type: NotificationType, 
        callback: Callable[[Notification], None]
    ) -> None:
        """
        Unregister a callback.
        
        Args:
            notification_type: Type of notification
            callback: Function to remove
        """
        with self._lock:
            if notification_type in self._callbacks:
                if callback in self._callbacks[notification_type]:
                    self._callbacks[notification_type].remove(callback)
    
    # =========================================================================
    # CONVENIENCE METHODS FOR COMMON NOTIFICATIONS
    # =========================================================================
    
    async def notify_system_start(self, state: SystemState) -> None:
        """
        Notify system startup.
        """
        notification = Notification(
            notification_type=NotificationType.SYSTEM_START,
            level=NotificationLevel.INFO,
            title="System Started",
            message=f"Rapid Fire Intraday Trading System started in {state.current_mode} mode",
            data={
                "mode": state.current_mode,
                "paper_trading": state.is_paper_trading,
                "start_time": state.start_time.isoformat() if state.start_time else "N/A"
            }
        )
        await self.send_notification(notification)
    
    async def notify_system_stop(self, state: SystemState) -> None:
        """
        Notify system shutdown.
        """
        duration = "N/A"
        if state.start_time:
            duration = str(datetime.now() - state.start_time)
        
        notification = Notification(
            notification_type=NotificationType.SYSTEM_STOP,
            level=NotificationLevel.INFO,
            title="System Stopped",
            message=f"Rapid Fire Intraday Trading System stopped after {duration}",
            data={
                "duration": duration,
                "stop_time": datetime.now().isoformat()
            }
        )
        await self.send_notification(notification)
    
    async def notify_trade_open(
        self, 
        symbol: str, 
        side: OrderSide, 
        quantity: float, 
        price: float,
        strategy: Optional[str] = None,
        confidence: Optional[float] = None
    ) -> None:
        """
        Notify new trade opened.
        """
        notification = Notification(
            notification_type=NotificationType.TRADE_OPEN,
            level=NotificationLevel.TRADE,
            title=f"Trade Opened: {symbol} {side.value}",
            message=f"{side.value} {quantity} {symbol} @ {price}",
            data={
                "symbol": symbol,
                "side": side.value,
                "quantity": quantity,
                "price": price,
                "strategy": strategy or "N/A",
                "confidence": f"{confidence:.2%}" if confidence else "N/A"
            }
        )
        await self.send_notification(notification)
    
    async def notify_trade_close(
        self, 
        trade: Trade
    ) -> None:
        """
        Notify trade closed.
        """
        # Check if we should notify based on PnL threshold
        abs_pnl = abs(trade.net_pnl)
        if abs_pnl < self.notification_config.trade_pnl_threshold:
            return
        
        pnl_sign = "+" if trade.net_pnl >= 0 else ""
        notification = Notification(
            notification_type=NotificationType.TRADE_CLOSE,
            level=NotificationLevel.TRADE if trade.net_pnl >= 0 else NotificationLevel.WARNING,
            title=f"Trade Closed: {trade.symbol}",
            message=f"{trade.side.value} {trade.quantity} {trade.symbol} - PnL: {pnl_sign}{trade.net_pnl:.2f}",
            data={
                "symbol": trade.symbol,
                "side": trade.side.value,
                "entry_price": trade.entry_price,
                "exit_price": trade.exit_price,
                "quantity": trade.quantity,
                "pnl": trade.net_pnl,
                "net_pnl": trade.net_pnl,
                "duration": f"{trade.duration_seconds:.0f}s",
                "return_pct": f"{trade.return_pct:.2f}%",
                "strategy": trade.strategy.value if trade.strategy else "N/A"
            }
        )
        await self.send_notification(notification)
    
    async def notify_stop_loss_hit(
        self, 
        symbol: str, 
        side: OrderSide, 
        sl_price: float,
        current_price: float,
        pnl: float
    ) -> None:
        """
        Notify stop loss hit.
        """
        notification = Notification(
            notification_type=NotificationType.TRADE_SL,
            level=NotificationLevel.WARNING,
            title=f"Stop Loss Hit: {symbol}",
            message=f"{side.value} position in {symbol} hit SL at {sl_price}, current: {current_price}, PnL: {pnl:.2f}",
            data={
                "symbol": symbol,
                "side": side.value,
                "sl_price": sl_price,
                "current_price": current_price,
                "pnl": pnl
            }
        )
        await self.send_notification(notification)
    
    async def notify_take_profit_hit(
        self, 
        symbol: str, 
        side: OrderSide, 
        tp_price: float,
        current_price: float,
        pnl: float
    ) -> None:
        """
        Notify take profit hit.
        """
        notification = Notification(
            notification_type=NotificationType.TRADE_TP,
            level=NotificationLevel.INFO,
            title=f"Take Profit Hit: {symbol}",
            message=f"{side.value} position in {symbol} hit TP at {tp_price}, current: {current_price}, PnL: {pnl:.2f}",
            data={
                "symbol": symbol,
                "side": side.value,
                "tp_price": tp_price,
                "current_price": current_price,
                "pnl": pnl
            }
        )
        await self.send_notification(notification)
    
    async def notify_order_placed(self, order: Order) -> None:
        """
        Notify order placed.
        """
        notification = Notification(
            notification_type=NotificationType.ORDER_PLACED,
            level=NotificationLevel.INFO,
            title=f"Order Placed: {order.symbol}",
            message=f"{order.order_side.value} {order.quantity} {order.symbol} @ {order.price or 'MARKET'}",
            data={
                "order_id": order.order_id,
                "symbol": order.symbol,
                "side": order.order_side.value,
                "type": order.order_type.value,
                "quantity": order.quantity,
                "price": order.price or "MARKET",
                "validity": order.validity.value
            }
        )
        await self.send_notification(notification)
    
    async def notify_order_filled(self, order: Order) -> None:
        """
        Notify order filled.
        """
        notification = Notification(
            notification_type=NotificationType.ORDER_FILLED,
            level=NotificationLevel.INFO,
            title=f"Order Filled: {order.symbol}",
            message=f"{order.order_side.value} {order.filled_quantity} {order.symbol} filled @ {order.average_fill_price}",
            data={
                "order_id": order.order_id,
                "symbol": order.symbol,
                "side": order.order_side.value,
                "filled_quantity": order.filled_quantity,
                "average_fill_price": order.average_fill_price,
                "remaining": order.remaining_quantity
            }
        )
        await self.send_notification(notification)
    
    async def notify_order_cancelled(self, order: Order, reason: str = "") -> None:
        """
        Notify order cancelled.
        """
        notification = Notification(
            notification_type=NotificationType.ORDER_CANCELLED,
            level=NotificationLevel.WARNING,
            title=f"Order Cancelled: {order.symbol}",
            message=f"Order {order.order_id} for {order.quantity} {order.symbol} cancelled{': ' + reason if reason else ''}",
            data={
                "order_id": order.order_id,
                "symbol": order.symbol,
                "side": order.order_side.value,
                "quantity": order.quantity,
                "reason": reason
            }
        )
        await self.send_notification(notification)
    
    async def notify_order_rejected(self, order: Order, reason: str = "") -> None:
        """
        Notify order rejected.
        """
        notification = Notification(
            notification_type=NotificationType.ORDER_REJECTED,
            level=NotificationLevel.ERROR,
            title=f"Order Rejected: {order.symbol}",
            message=f"Order {order.order_id} for {order.quantity} {order.symbol} rejected{': ' + reason if reason else ''}",
            data={
                "order_id": order.order_id,
                "symbol": order.symbol,
                "side": order.order_side.value,
                "quantity": order.quantity,
                "reason": reason
            }
        )
        await self.send_notification(notification)
    
    async def notify_signal(self, signal: AggregatedSignal) -> None:
        """
        Notify new signal generated.
        """
        notification = Notification(
            notification_type=NotificationType.SIGNAL_GENERATED,
            level=NotificationLevel.INFO,
            title=f"Signal: {signal.final_signal.value} {signal.symbol}",
            message=f"Net score: {signal.net_score:.2f}, Confidence: {signal.net_confidence:.2f}",
            data={
                "symbol": signal.symbol,
                "signal": signal.final_signal.value,
                "net_score": signal.net_score,
                "net_confidence": signal.net_confidence,
                "regime_filter_passed": signal.regime_filter_passed,
                "strategies": len(signal.contributing_strategies)
            }
        )
        await self.send_notification(notification)
    
    async def notify_risk_alert(
        self, 
        risk_state: RiskState,
        alert_type: str = "generic"
    ) -> None:
        """
        Notify risk alert.
        """
        message = ""
        
        if alert_type == "daily_loss_limit":
            message = f"Daily loss limit breached! Daily PnL: {risk_state.daily_pnl_pct:.2f}%"
        elif alert_type == "daily_profit_limit":
            message = f"Daily profit limit reached! Daily PnL: {risk_state.daily_pnl_pct:.2f}%"
        elif alert_type == "max_positions":
            message = f"Maximum positions reached: {risk_state.open_positions_count}/{self.config.risk.max_concurrent_positions}"
        elif alert_type == "drawdown":
            message = f"Drawdown alert! Current drawdown: {risk_state.daily_pnl_pct:.2f}%"
        else:
            message = f"Risk alert: {risk_state.to_dict()}"
        
        notification = Notification(
            notification_type=NotificationType.RISK_ALERT,
            level=NotificationLevel.WARNING,
            title=f"Risk Alert: {alert_type}",
            message=message,
            data=risk_state.to_dict()
        )
        await self.send_notification(notification)
    
    async def notify_kill_switch(self, reason: str) -> None:
        """
        Notify kill switch activation.
        """
        notification = Notification(
            notification_type=NotificationType.KILL_SWITCH,
            level=NotificationLevel.CRITICAL,
            title="🚨 KILL SWITCH ACTIVATED 🚨",
            message=f"All positions closed and orders cancelled. Reason: {reason}",
            data={"reason": reason}
        )
        await self.send_notification(notification)
    
    async def notify_system_error(self, error: Exception, component: str = "unknown") -> None:
        """
        Notify system error.
        """
        notification = Notification(
            notification_type=NotificationType.SYSTEM_ERROR,
            level=NotificationLevel.ERROR,
            title=f"System Error in {component}",
            message=f"Error: {str(error)}",
            data={
                "component": component,
                "error": str(error),
                "error_type": type(error).__name__
            }
        )
        await self.send_notification(notification)
    
    async def notify_daily_summary(self, portfolio_state: PortfolioState) -> None:
        """
        Notify daily summary.
        """
        total_pnl = portfolio_state.realized_pnl
        pnl_pct = (total_pnl / self.config.trading.initial_capital * 100) if self.config.trading.initial_capital > 0 else 0
        
        notification = Notification(
            notification_type=NotificationType.DAILY_SUMMARY,
            level=NotificationLevel.INFO,
            title="Daily Trading Summary",
            message=f"Trades: {len(portfolio_state.closed_trades)}, PnL: {total_pnl:.2f} ({pnl_pct:.2f}%)",
            data={
                "total_trades": len(portfolio_state.closed_trades),
                "winning_trades": len([t for t in portfolio_state.closed_trades if t.net_pnl > 0]),
                "losing_trades": len([t for t in portfolio_state.closed_trades if t.net_pnl < 0]),
                "total_pnl": total_pnl,
                "pnl_pct": pnl_pct,
                "equity": portfolio_state.equity,
                "drawdown": portfolio_state.unrealized_pnl({})  # Simplified
            }
        )
        await self.send_notification(notification)
    
    async def notify_portfolio_update(self, portfolio_state: PortfolioState) -> None:
        """
        Notify portfolio update.
        """
        # Only notify if daily PnL change exceeds threshold
        daily_pnl = portfolio_state.realized_pnl
        daily_pnl_pct = abs(daily_pnl / self.config.trading.initial_capital * 100) if self.config.trading.initial_capital > 0 else 0
        
        if daily_pnl_pct < self.notification_config.daily_pnl_threshold:
            return
        
        notification = Notification(
            notification_type=NotificationType.PORTFOLIO_UPDATE,
            level=NotificationLevel.INFO,
            title="Portfolio Update",
            message=f"Equity: {portfolio_state.equity:.2f}, Open Positions: {portfolio_state.num_open_positions}",
            data={
                "equity": portfolio_state.equity,
                "cash": portfolio_state.cash,
                "open_positions": portfolio_state.num_open_positions,
                "daily_pnl": daily_pnl
            }
        )
        await self.send_notification(notification)


# Global notifier instance
_global_notifier: Optional[Notifier] = None


def get_notifier(config: Config) -> Notifier:
    """
    Get or create the global notifier instance.
    
    Args:
        config: System configuration
        
    Returns:
        Notifier instance
    """
    global _global_notifier
    
    if _global_notifier is None:
        _global_notifier = Notifier(config)
    
    return _global_notifier