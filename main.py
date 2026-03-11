"""
AstrBot 智能主动回复插件
基于人设、对话上下文、时间规则实现的全场景智能主动回复
"""
import asyncio
import uuid
import random
import json
import re
from datetime import datetime
from typing import Dict, Any, Optional

from astrbot.api import logger
from astrbot.api.star import Context, Star, StarTools
from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api import AstrBotConfig

from .database import DatabaseManager
from .scheduler import TaskScheduler

# 常量定义
CONVERSATION_SUMMARY_COOLDOWN_HOURS = 1  # 对话总结冷却时间（小时）
INACTIVE_1D_HOURS = 24  # 1天未活跃阈值（小时）
INACTIVE_6D_HOURS = 144  # 6天未活跃阈值（小时）
MIN_EVENT_INTERVAL_HOURS = 4  # 事件最小间隔（小时）
LLM_TIMEOUT_SECONDS = 100.0  # LLM调用超时时间（秒）
MESSAGE_FREQUENCY_CHECK_HOURS = 1  # 频率限制检查时间窗口（小时）
OLD_RECORDS_RETENTION_DAYS = 7  # 旧记录保留天数
_LLM_CONCURRENCY = 5  # LLM 并发度，与 Semaphore 保持一致


class ProactiveReplyPlugin(Star):
    """智能主动回复插件"""

    def __init__(self, context: Context, config: AstrBotConfig = None):
        """
        初始化插件

        Args:
            context: AstrBot 上下文
            config: 插件配置（可选）
        """
        super().__init__(context)

        logger.info("[ProactiveReply] [插件初始化] 开始初始化智能主动回复插件")

        # 保存配置
        self.config = config if config is not None else {}

        # 验证配置
        self._validate_config()

        # 初始化数据库
        db_path = StarTools.get_data_dir() / "proactive_reply.db"
        self.db = DatabaseManager(db_path)

        # 初始化调度器
        self.scheduler = TaskScheduler()

        # 对话结束倒计时任务字典 {user_id: asyncio.Task}
        self.conversation_timers: Dict[str, asyncio.Task] = {}

        # LLM 并发控制信号量
        self._llm_semaphore = asyncio.Semaphore(_LLM_CONCURRENCY)

        # 每用户发送频控锁，保证 check+record 原子性
        self._send_locks: Dict[str, asyncio.Lock] = {}

        # 初始化状态标志
        self._initialized = False

        # 直接调度异步初始化，兼容插件重载场景；保留强引用防止被 GC 提前回收
        self._init_task = asyncio.create_task(self._async_init())

        logger.info("[ProactiveReply] [插件初始化] 插件初始化完成")

    def _validate_config(self):
        """验证配置项，并在验证前预填所有默认值，确保业务逻辑可直接用 self.config['key']"""
        # 预填默认值：所有配置键在此统一声明，业务代码无需再携带硬编码默认值
        _DEFAULTS = {
            "enable_plugin": True,
            "enable_group_proactive": False,
            "enable_qq_poke": False,
            "enable_inactive_1d_touch": True,
            "enable_inactive_6d_touch": True,
            "disturb_free_start": 0,
            "disturb_free_end": 6,
            "greeting_probabilities": [0.6, 0.2, 0.0],
            "daily_max_greeting_count": 2,
            "daily_max_event_count": 2,
            "qq_msg_frequency_limit": 2,
            "conversation_end_threshold": 10,
            "proactive_msg_min_interval": 2,
            "conversation_summary_max_limit": 20,
            "inactive_scan_time": "08:00",
            "daily_event_generate_time": "01:00",
            "daily_greeting_plan_time": "00:30",
            "greeting_time_ranges": [
                {"name": "早间", "start": 6, "end": 8},
                {"name": "午间", "start": 12, "end": 12},
                {"name": "晚间", "start": 18, "end": 20},
            ],
        }
        for key, default in _DEFAULTS.items():
            if key not in self.config:
                self.config[key] = default

        try:
            # 验证防打扰时段
            disturb_start = self.config.get("disturb_free_start", 0)
            disturb_end = self.config.get("disturb_free_end", 6)
            if not (0 <= disturb_start < 24 and 0 <= disturb_end < 24):
                logger.warning(f"[ProactiveReply] [配置验证] 防打扰时段配置无效（{disturb_start}-{disturb_end}），使用默认值（0-6）")
                self.config["disturb_free_start"] = 0
                self.config["disturb_free_end"] = 6

            # 验证问候概率
            probs = self.config.get("greeting_probabilities", [0.6, 0.2, 0.0])
            if not isinstance(probs, list) or len(probs) != 3:
                logger.warning("[ProactiveReply] [配置验证] 问候概率配置无效，使用默认值")
                self.config["greeting_probabilities"] = [0.6, 0.2, 0.0]
            else:
                # 验证概率值范围
                for i, prob in enumerate(probs):
                    if not (0 <= prob <= 1):
                        logger.warning(f"[ProactiveReply] [配置验证] 问候概率[{i}]超出范围（{prob}），设置为0")
                        probs[i] = 0

            # 验证每日最大次数
            max_greeting = self.config.get("daily_max_greeting_count", 2)
            if max_greeting > 2:
                logger.warning(f"[ProactiveReply] [配置验证] 每日问候次数超过限制（{max_greeting}），设置为2")
                self.config["daily_max_greeting_count"] = 2
            elif max_greeting < 0:
                logger.warning(f"[ProactiveReply] [配置验证] 每日问候次数无效（{max_greeting}），设置为2")
                self.config["daily_max_greeting_count"] = 2

            max_event = self.config.get("daily_max_event_count", 2)
            if max_event > 2:
                logger.warning(f"[ProactiveReply] [配置验证] 每日事件次数超过限制（{max_event}），设置为2")
                self.config["daily_max_event_count"] = 2
            elif max_event < 0:
                logger.warning(f"[ProactiveReply] [配置验证] 每日事件次数无效（{max_event}），设置为2")
                self.config["daily_max_event_count"] = 2

            # 验证频率限制
            freq_limit = self.config.get("qq_msg_frequency_limit", 2)
            if freq_limit < 1:
                logger.warning(f"[ProactiveReply] [配置验证] 频率限制无效（{freq_limit}），设置为2")
                self.config["qq_msg_frequency_limit"] = 2
            elif freq_limit > 5:
                logger.warning(f"[ProactiveReply] [配置验证] 频率限制过高（{freq_limit}），设置为5")
                self.config["qq_msg_frequency_limit"] = 5

            # 验证对话结束阈值
            threshold = self.config.get("conversation_end_threshold", 10)
            if threshold < 1 or threshold > 30:
                logger.warning(f"[ProactiveReply] [配置验证] 对话结束阈值无效（{threshold}分钟），设置为10分钟")
                self.config["conversation_end_threshold"] = 10

            # 验证消息间隔
            interval = self.config.get("proactive_msg_min_interval", 2)
            if interval < 1 or interval > 24:
                logger.warning(f"[ProactiveReply] [配置验证] 消息最小间隔无效（{interval}小时），设置为2小时")
                self.config["proactive_msg_min_interval"] = 2

            # 验证时间格式配置
            time_configs = {
                "inactive_scan_time": "08:00",
                "daily_event_generate_time": "01:00",
                "daily_greeting_plan_time": "00:30"
            }
            for time_key, default_value in time_configs.items():
                time_str = self.config.get(time_key, default_value)
                try:
                    datetime.strptime(time_str, "%H:%M")
                except ValueError:
                    logger.warning(f"[ProactiveReply] [配置验证] 时间格式无效（{time_key}={time_str}），使用默认值（{default_value}）")
                    self.config[time_key] = default_value

            # 验证问候时段配置
            time_ranges = self.config.get("greeting_time_ranges", [])
            if not isinstance(time_ranges, list):
                logger.warning("[ProactiveReply] [配置验证] 问候时段配置格式错误，使用默认值")
                self.config["greeting_time_ranges"] = [
                    {"name": "早间", "start": 6, "end": 8},
                    {"name": "午间", "start": 12, "end": 12},
                    {"name": "晚间", "start": 18, "end": 20}
                ]
            else:
                # 验证每个时段的结构
                valid_ranges = []
                for tr in time_ranges:
                    if isinstance(tr, dict) and "start" in tr and "end" in tr:
                        start = tr.get("start", 0)
                        end = tr.get("end", 0)
                        if 0 <= start < 24 and 0 <= end < 24:
                            valid_ranges.append(tr)
                        else:
                            logger.warning(f"[ProactiveReply] [配置验证] 时段配置无效（start={start}, end={end}），跳过")
                    else:
                        logger.warning("[ProactiveReply] [配置验证] 时段配置格式错误，跳过")

                if valid_ranges:
                    self.config["greeting_time_ranges"] = valid_ranges
                else:
                    logger.warning("[ProactiveReply] [配置验证] 所有时段配置无效，使用默认值")
                    self.config["greeting_time_ranges"] = [
                        {"name": "早间", "start": 6, "end": 8},
                        {"name": "午间", "start": 12, "end": 12},
                        {"name": "晚间", "start": 18, "end": 20}
                    ]

            logger.info("[ProactiveReply] [配置验证] 配置验证完成")

        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"[ProactiveReply] [配置验证] 配置验证失败: {e}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [配置验证] 配置验证发生未知错误: {e}")

    def _extract_json_from_text(self, text: str) -> Optional[str]:
        """从文本中提取 JSON，处理 markdown 代码块"""
        # 先尝试提取 markdown 代码块中的内容
        code_block_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
        if code_block_match:
            return code_block_match.group(1)

        # 使用括号平衡扫描提取第一个完整 JSON 对象，避免贪婪匹配跨越多个块
        start = text.find('{')
        if start == -1:
            return None
        depth = 0
        in_string = False
        escape_next = False
        for i, ch in enumerate(text[start:], start):
            if escape_next:
                escape_next = False
                continue
            if ch == '\\' and in_string:
                escape_next = True
                continue
            if ch == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    return text[start:i + 1]
        return None

    async def _async_init(self):
        """异步初始化任务"""
        try:
            # 初始化数据库
            await self.db.initialize()

            # 启动调度器
            await self.scheduler.start()

            # 注册全局定时任务
            await self._register_global_tasks()

            # 恢复未执行的任务
            await self._restore_pending_tasks()

            # 标记初始化完成
            self._initialized = True

            logger.info("[ProactiveReply] [插件初始化] 异步初始化完成")
        except (OSError, RuntimeError) as e:
            logger.error(f"[ProactiveReply] [插件初始化] 异步初始化失败: {e}")
            self._initialized = False
        except Exception as e:
            logger.error(f"[ProactiveReply] [插件初始化] 异步初始化发生未知错误: {e}")
            self._initialized = False

    async def _register_global_tasks(self):
        """注册全局定时任务"""
        try:
            # 每日问候计划生成
            greeting_time = self.config["daily_greeting_plan_time"]
            self.scheduler.add_daily_task(
                "daily_greeting_plan",
                greeting_time,
                self._generate_daily_greeting_plan
            )

            # 每日事件生成
            event_time = self.config["daily_event_generate_time"]
            self.scheduler.add_daily_task(
                "daily_event_generate",
                event_time,
                self._generate_daily_events
            )

            # 未活跃用户扫描
            scan_time = self.config["inactive_scan_time"]
            self.scheduler.add_daily_task(
                "inactive_user_scan",
                scan_time,
                self._scan_inactive_users
            )

            # 每日计数器重置（凌晨0点）
            self.scheduler.add_daily_task(
                "daily_counter_reset",
                "00:00",
                self.db.reset_daily_counters
            )

            # 清理旧消息记录（每天凌晨2点）
            self.scheduler.add_daily_task(
                "cleanup_old_records",
                "02:00",
                self._cleanup_old_records
            )

            # 清理过期的对话倒计时任务（每天凌晨4点）
            self.scheduler.add_daily_task(
                "cleanup_conversation_timers",
                "04:00",
                self._cleanup_conversation_timers
            )

            logger.info("[ProactiveReply] [插件初始化] 全局定时任务注册完成")
        except (ValueError, KeyError) as e:
            logger.error(f"[ProactiveReply] [插件初始化] 注册全局定时任务失败: {e}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [插件初始化] 注册全局定时任务发生未知错误: {e}")

    async def _restore_pending_tasks(self):
        """恢复未执行的待办任务"""
        try:
            tasks = await self.db.get_pending_tasks()
            now = self.scheduler.get_beijing_time()
            restored_count = 0
            expired_count = 0

            for task in tasks:
                trigger_time = datetime.fromtimestamp(task['trigger_time'], self.scheduler.beijing_tz)

                # 检查任务是否过期
                if trigger_time <= now:
                    await self.db.update_task_status(task['task_id'], 3, "任务已过期")
                    expired_count += 1
                    continue

                # 检查任务是否已在调度器中（get_job 不存在时返回 None）
                if self.scheduler.scheduler.get_job(task['task_id']) is not None:
                    logger.debug(f"[ProactiveReply] [插件初始化] 任务[{task['task_id']}]已存在，跳过")
                    continue

                # 恢复任务
                if self.scheduler.add_one_time_task(
                    task['task_id'],
                    trigger_time,
                    self._execute_task,
                    task['task_id']
                ):
                    restored_count += 1

            logger.info(f"[ProactiveReply] [插件初始化] 任务恢复完成，恢复{restored_count}个任务，过期{expired_count}个任务")
        except (ValueError, KeyError, TypeError) as e:
            logger.error(f"[ProactiveReply] [插件初始化] 恢复待办任务失败: {e}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [插件初始化] 恢复待办任务发生未知错误: {e}")

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        """
        监听所有消息事件
        更新用户活跃状态，触发对话结束判定
        """
        try:
            # 检查初始化是否完成
            if not self._initialized:
                logger.debug("[ProactiveReply] [事件监听] 插件尚未初始化完成，跳过处理")
                return

            # 检查插件是否启用
            if not self.config["enable_plugin"]:
                return

            # 检查是否是群聊消息（使用文档推荐的 get_group_id 方法）
            if event.get_group_id() and not self.config["enable_group_proactive"]:
                return

            # 过滤机器人自身发出的消息，避免污染用户活跃状态
            if getattr(event, 'is_self_message', False):
                return
            sender_id = getattr(event, 'sender_id', None) or getattr(event, 'sender', None)
            self_id = getattr(event, 'self_id', None)
            if sender_id and self_id and str(sender_id) == str(self_id):
                return

            user_id = event.unified_msg_origin

            # 更新用户活跃时间
            await self.db.update_user_active_time(user_id)

            logger.info(f"[ProactiveReply] [事件监听] 收到用户[{user_id[:16]}...]的消息，启动对话结束倒计时")

            # 取消之前的倒计时，同时清理已完成的任务避免内存膨胀
            done_users = [uid for uid, t in self.conversation_timers.items() if t.done()]
            for uid in done_users:
                del self.conversation_timers[uid]

            if user_id in self.conversation_timers:
                self.conversation_timers[user_id].cancel()

            # 取消该用户所有待执行的 conversation_end 类型任务
            await self._cancel_user_conversation_tasks(user_id)

            # 启动新的倒计时
            threshold = self.config["conversation_end_threshold"] * 60  # 转换为秒
            self.conversation_timers[user_id] = asyncio.create_task(
                self._conversation_end_timer(user_id, threshold)
            )

        except (AttributeError, KeyError) as e:
            logger.error(f"[ProactiveReply] [事件监听] 处理消息事件失败: {e}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [事件监听] 处理消息事件发生未知错误: {e}")

    async def _cleanup_old_records(self):
        """清理旧消息发送记录（定时任务回调）"""
        await self.db.cleanup_old_send_records(days=OLD_RECORDS_RETENTION_DAYS)

    async def _cleanup_conversation_timers(self):
        """清理已完成的对话倒计时任务，同时回收无活跃用户的发送锁"""
        try:
            # 清理已完成的任务
            completed_users = [
                user_id for user_id, task in self.conversation_timers.items()
                if task.done()
            ]

            for user_id in completed_users:
                del self.conversation_timers[user_id]

            if completed_users:
                logger.info(f"[ProactiveReply] [定时任务] 清理了{len(completed_users)}个已完成的对话倒计时任务")

            # 回收不再有活跃计时器的用户发送锁，防止长期运行内存增长
            active_users = set(self.conversation_timers.keys())
            stale_lock_users = [uid for uid in list(self._send_locks.keys()) if uid not in active_users]
            for uid in stale_lock_users:
                lock = self._send_locks.get(uid)
                if lock and not lock.locked():
                    del self._send_locks[uid]
            if stale_lock_users:
                logger.debug(f"[ProactiveReply] [定时任务] 回收了{len(stale_lock_users)}个无活跃用户的发送锁")
        except Exception as e:
            logger.error(f"[ProactiveReply] [定时任务] 清理对话倒计时任务失败: {e}")

    async def _conversation_end_timer(self, user_id: str, threshold: int):
        """
        对话结束倒计时

        Args:
            user_id: 用户ID
            threshold: 倒计时秒数
        """
        try:
            await asyncio.sleep(threshold)

            # 检查冷却时间（从数据库读取，重启后仍有效）
            now = self.scheduler.get_beijing_time().timestamp()
            last_summary_time = await self.db.get_conversation_summary_cooldown(user_id)
            cooldown_seconds = CONVERSATION_SUMMARY_COOLDOWN_HOURS * 3600

            if now - last_summary_time < cooldown_seconds:
                remaining_minutes = int((cooldown_seconds - (now - last_summary_time)) / 60)
                logger.info(f"[ProactiveReply] [对话判定] 用户[{user_id[:16]}...]对话结束，但在冷却期内（还需{remaining_minutes}分钟），跳过总结")
                return

            logger.info(f"[ProactiveReply] [对话判定] 用户[{user_id[:16]}...]对话结束，触发上下文总结流程")

            # 触发对话结束后的主动回复流程，仅成功后才写入冷却时间
            await self._handle_conversation_end(user_id)

            # 持久化本次总结时间（放在成功执行后，避免失败时误触发冷却）
            await self.db.set_conversation_summary_cooldown(user_id, int(now))

        except asyncio.CancelledError:
            logger.info(f"[ProactiveReply] [对话判定] 用户[{user_id[:16]}...]有新消息，重置对话结束倒计时")
        except (RuntimeError, ValueError) as e:
            logger.error(f"[ProactiveReply] [对话判定] 对话结束倒计时失败: {e}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [对话判定] 对话结束倒计时发生未知错误: {e}")
        finally:
            # 仅删除当前任务自身对应的映射，避免误删新计时任务
            current_task = asyncio.current_task()
            if self.conversation_timers.get(user_id) is current_task:
                del self.conversation_timers[user_id]

    async def _cancel_user_conversation_tasks(self, user_id: str):
        """
        取消用户所有待执行的对话结束类型任务

        Args:
            user_id: 用户ID
        """
        try:
            # 获取该用户所有待执行的 conversation_end 类型任务
            pending_tasks = await self.db.get_pending_tasks(user_id)

            cancelled_count = 0
            for task in pending_tasks:
                if task['task_type'] == 'conversation_end':
                    # 从调度器移除任务
                    self.scheduler.remove_task(task['task_id'])
                    # 更新数据库状态为已取消
                    await self.db.update_task_status(task['task_id'], 3, "用户已主动发消息")
                    cancelled_count += 1

            if cancelled_count > 0:
                logger.info(f"[ProactiveReply] [任务取消] 用户[{user_id[:16]}...]主动发消息，已取消{cancelled_count}个待执行的对话结束任务")

        except (KeyError, TypeError) as e:
            logger.error(f"[ProactiveReply] [任务取消] 取消任务失败: {e}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [任务取消] 取消任务发生未知错误: {e}")

    async def _handle_conversation_end(self, user_id: str):
        """
        处理对话结束后的主动回复

        Args:
            user_id: 用户ID
        """
        try:
            # 1. 获取对话历史
            conv_mgr = self.context.conversation_manager
            curr_cid = await conv_mgr.get_curr_conversation_id(user_id)

            if not curr_cid:
                logger.warning(f"[ProactiveReply] [上下文处理] 用户[{user_id[:16]}...]没有对话历史，跳过")
                return

            conversation = await conv_mgr.get_conversation(user_id, curr_cid)

            if not conversation or not conversation.history:
                logger.warning(f"[ProactiveReply] [上下文处理] 用户[{user_id[:16]}...]对话历史为空，跳过")
                return

            # 2. 生成对话总结
            # 文档明确 conversation.history 是 str 类型
            history_text = conversation.history

            if not history_text or not history_text.strip():
                logger.warning(f"[ProactiveReply] [上下文处理] 用户[{user_id[:16]}...]对话历史为空，跳过")
                return

            summary = await self._generate_conversation_summary(user_id, history_text)

            if not summary:
                logger.warning(f"[ProactiveReply] [上下文处理] 用户[{user_id[:16]}...]对话总结生成失败")
                return

            logger.info(f"[ProactiveReply] [上下文处理] 用户[{user_id[:16]}...]对话总结完成")

            # 3. LLM 决策是否需要主动回复
            decision = await self._llm_decide_proactive_reply(user_id, summary)

            if not decision or not decision.get("need_reply"):
                logger.info(f"[ProactiveReply] [任务注册] 用户[{user_id[:16]}...]无需发起主动回复，流程结束")
                return

            # 4. 注册主动回复任务
            await self._register_proactive_task(user_id, decision, summary, "conversation_end")

        except (AttributeError, KeyError, TypeError) as e:
            logger.error(f"[ProactiveReply] [对话结束处理] 处理失败: {e}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [对话结束处理] 处理发生未知错误: {e}")

    async def _generate_conversation_summary(self, user_id: str, history: str) -> Optional[str]:
        """
        生成对话总结

        Args:
            user_id: 用户ID
            history: 对话历史（字符串格式）

        Returns:
            对话总结文本
        """
        try:
            # history 是字符串格式，直接使用
            if not history or len(history.strip()) == 0:
                logger.warning("[ProactiveReply] [上下文处理] 对话历史为空")
                return None

            # 限制历史长度（按字符数）
            max_chars = self.config["conversation_summary_max_limit"] * 100  # 每条消息约100字符
            if len(history) > max_chars:
                history = history[-max_chars:]  # 取最后N字符

            prompt = f"""请总结以下对话的核心内容，提炼用户的状态、计划、情绪等关键信息：

{history}

请用1-2句话简洁总结。"""

            # 调用 LLM（添加超时控制）
            provider_id = await self.context.get_current_chat_provider_id(umo=user_id)
            try:
                llm_resp = await asyncio.wait_for(
                    self.context.llm_generate(
                        chat_provider_id=provider_id,
                        prompt=prompt
                    ),
                    timeout=LLM_TIMEOUT_SECONDS
                )
                return llm_resp.completion_text
            except asyncio.TimeoutError:
                logger.error(f"[ProactiveReply] [上下文处理] LLM调用超时（{LLM_TIMEOUT_SECONDS}秒）")
                return None

        except (AttributeError, KeyError, TypeError) as e:
            logger.error(f"[ProactiveReply] [上下文处理] 生成对话总结失败: {e}", exc_info=True)
            # 兜底：使用最后200字符
            try:
                if len(history) > 200:
                    return "对话内容：" + history[-200:]
                else:
                    return "对话内容：" + history
            except (TypeError, AttributeError) as fallback_error:
                logger.error(f"[ProactiveReply] [上下文处理] 兜底逻辑也失败: {fallback_error}", exc_info=True)
            return None

    async def _llm_decide_proactive_reply(self, user_id: str, summary: str) -> Optional[Dict[str, Any]]:
        """
        LLM 决策是否需要主动回复及触发时间

        Args:
            user_id: 用户ID
            summary: 对话总结

        Returns:
            决策结果 {"need_reply": bool, "trigger_time": str}
        """
        try:
            now = self.scheduler.get_beijing_time()

            prompt = f"""基于以下对话总结，判断是否需要发起主动回复，以及最佳的触发时间。

对话总结：{summary}

当前北京时间：{now.strftime('%Y-%m-%d %H:%M')}

请判断：
1. 是否需要发起主动回复？（例如用户提到"去吃饭了""去忙了""晚点聊"等未来场景）
2. 如果需要，最佳的触发时间是什么？（格式：YYYY-MM-DD HH:MM，北京时间）

请以JSON格式回复：
{{"need_reply": true/false, "trigger_time": "YYYY-MM-DD HH:MM", "reason": "原因"}}"""

            provider_id = await self.context.get_current_chat_provider_id(umo=user_id)
            try:
                llm_resp = await asyncio.wait_for(
                    self.context.llm_generate(
                        chat_provider_id=provider_id,
                        prompt=prompt
                    ),
                    timeout=LLM_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                logger.error(f"[ProactiveReply] [LLM决策] LLM调用超时（{LLM_TIMEOUT_SECONDS}秒）")
                return None

            # 解析 JSON 响应
            json_str = self._extract_json_from_text(llm_resp.completion_text)
            if json_str:
                try:
                    decision = json.loads(json_str)

                    # 验证必需字段
                    if not isinstance(decision.get('need_reply'), bool):
                        logger.error("[ProactiveReply] [LLM决策] 决策结果缺少或无效 need_reply 字段")
                        return None

                    if decision['need_reply']:
                        if not decision.get('trigger_time'):
                            logger.error("[ProactiveReply] [LLM决策] 决策结果缺少 trigger_time 字段")
                            return None
                        # 验证时间格式
                        try:
                            datetime.strptime(decision['trigger_time'], "%Y-%m-%d %H:%M")
                        except ValueError:
                            logger.error(f"[ProactiveReply] [LLM决策] trigger_time 格式无效: {decision.get('trigger_time')}")
                            return None

                    logger.info(f"[ProactiveReply] [LLM决策] 用户[{user_id[:16]}...]主动回复决策完成，"
                               f"是否触发：{decision.get('need_reply')}，"
                               f"触发时间：{decision.get('trigger_time', 'N/A')}（北京时间）")
                    return decision

                except json.JSONDecodeError as e:
                    logger.error(f"[ProactiveReply] [LLM决策] JSON解析失败: {e}")
                    return None
            else:
                logger.error("[ProactiveReply] [LLM决策] 无法从响应中提取JSON")

            return None

        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"[ProactiveReply] [LLM决策] 决策失败: {e}")
            return None

    async def _register_proactive_task(self, user_id: str, decision: Dict[str, Any],
                                      context_data: str, task_type: str):
        """
        注册主动回复任务

        Args:
            user_id: 用户ID
            decision: LLM 决策结果
            context_data: 上下文数据
            task_type: 任务类型
        """
        try:
            # 解析触发时间
            trigger_time_str = decision.get("trigger_time")
            if not trigger_time_str:
                logger.warning("[ProactiveReply] [任务注册] 触发时间为空，跳过")
                return

            trigger_time = datetime.strptime(trigger_time_str, "%Y-%m-%d %H:%M")
            trigger_time = self.scheduler.beijing_tz.localize(trigger_time)

            # 时间校验：避开防打扰时段
            disturb_start = self.config["disturb_free_start"]
            disturb_end = self.config["disturb_free_end"]
            trigger_time = self.scheduler.adjust_time_avoid_disturb(trigger_time, disturb_start, disturb_end)

            # 时间校验：检查与已有任务的间隔
            existing_tasks = await self.db.get_pending_tasks(user_id)
            existing_times = [
                datetime.fromtimestamp(task['trigger_time'], self.scheduler.beijing_tz)
                for task in existing_tasks
            ]

            min_interval = self.config["proactive_msg_min_interval"]
            trigger_time = self.scheduler.check_task_interval(trigger_time, existing_times, min_interval)

            # 再次检查防打扰时段（因为间隔调整可能导致时间落入防打扰时段）
            trigger_time = self.scheduler.adjust_time_avoid_disturb(trigger_time, disturb_start, disturb_end)

            # 生成任务ID
            task_id = f"{task_type}_{user_id}_{uuid.uuid4().hex[:8]}"

            # 保存任务到数据库，失败则终止后续调度器注册
            task_context = {
                "summary": context_data,
                "reason": decision.get("reason", "")
            }
            db_ok = await self.db.add_task(
                task_id,
                user_id,
                int(trigger_time.timestamp()),
                task_type,
                task_context
            )
            if not db_ok:
                logger.error(f"[ProactiveReply] [任务注册] 任务[{task_id}]数据库保存失败，终止注册")
                return

            # 注册到调度器，失败则将 DB 任务标记为取消，避免悬挂 pending 记录
            if not self.scheduler.add_one_time_task(
                task_id,
                trigger_time,
                self._execute_task,
                task_id
            ):
                logger.warning(f"[ProactiveReply] [任务注册] 任务[{task_id}]调度器注册失败，标记为取消")
                await self.db.update_task_status(task_id, 3, "调度器注册失败")

        except (ValueError, KeyError, TypeError) as e:
            logger.error(f"[ProactiveReply] [任务注册] 注册任务失败: {e}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [任务注册] 注册任务发生未知错误: {e}")

    async def _execute_task(self, task_id: str):
        """
        执行任务

        Args:
            task_id: 任务ID
        """
        try:
            # 获取任务信息（直接通过 task_id 查询）
            tasks = await self.db.get_pending_tasks(task_id=task_id)

            if not tasks:
                logger.warning(f"[ProactiveReply] [任务执行] 任务[{task_id}]不存在")
                return

            task = tasks[0]

            logger.info(f"[ProactiveReply] [任务执行] 开始执行用户[{task['user_id'][:16]}...]的任务[{task_id}]，"
                       f"任务类型：{task['task_type']}")

            # 生成主动消息
            message = await self._generate_proactive_message(task)

            if not message:
                await self.db.update_task_status(task_id, 2, "消息生成失败")
                return

            # 发送消息
            success = await self._send_proactive_message(task['user_id'], message)

            if success:
                await self.db.update_task_status(task_id, 1)
                logger.info(f"[ProactiveReply] [消息发送] 用户[{task['user_id'][:16]}...]的任务[{task_id}]执行成功，消息已发送")
            else:
                await self.db.update_task_status(task_id, 2, "消息发送失败")

        except (KeyError, TypeError, AttributeError) as e:
            logger.error(f"[ProactiveReply] [任务执行] 执行任务失败: {e}")
            try:
                await self.db.update_task_status(task_id, 2, str(e))
            except Exception as db_error:
                logger.error(f"[ProactiveReply] [任务执行] 更新任务状态失败: {db_error}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [任务执行] 执行任务发生未知错误: {e}")
            try:
                await self.db.update_task_status(task_id, 2, str(e))
            except Exception as db_error:
                logger.error(f"[ProactiveReply] [任务执行] 更新任务状态失败: {db_error}")

    async def _generate_proactive_message(self, task: Dict[str, Any]) -> Optional[str]:
        """
        生成主动消息内容

        Args:
            task: 任务信息

        Returns:
            消息文本
        """
        try:
            user_id = task['user_id']
            task_context = task.get('task_context', {})

            # 获取人设
            persona_mgr = self.context.persona_manager
            persona = await persona_mgr.get_default_persona_v3(umo=user_id)
            persona_prompt = persona.prompt if persona and hasattr(persona, 'prompt') else ""

            # 构建提示词
            prompt = f"""你获得了一次主动回复的机会，请保持人设不变，结合以下内容生成一条自然的主动消息。

人设：{persona_prompt}

对话背景：{task_context.get('summary', '')}

请生成一条简短、自然的主动消息（不超过50字）。"""

            # 调用 LLM，失败最多重试2次
            provider_id = await self.context.get_current_chat_provider_id(umo=user_id)
            max_retries = 2
            for attempt in range(max_retries + 1):
                try:
                    llm_resp = await asyncio.wait_for(
                        self.context.llm_generate(
                            chat_provider_id=provider_id,
                            prompt=prompt
                        ),
                        timeout=LLM_TIMEOUT_SECONDS
                    )
                    return llm_resp.completion_text.strip()
                except asyncio.TimeoutError:
                    logger.warning(f"[ProactiveReply] [消息生成] LLM调用超时（第{attempt + 1}次，{LLM_TIMEOUT_SECONDS}秒）")
                    if attempt < max_retries:
                        await asyncio.sleep(2)
                    else:
                        logger.error(f"[ProactiveReply] [消息生成] LLM调用超时，已重试{max_retries}次，放弃")
                        return None
                except Exception as llm_err:
                    logger.warning(f"[ProactiveReply] [消息生成] LLM调用失败（第{attempt + 1}次）: {llm_err}")
                    if attempt < max_retries:
                        await asyncio.sleep(2)
                    else:
                        logger.error(f"[ProactiveReply] [消息生成] LLM调用失败，已重试{max_retries}次，放弃")
                        return None
            return None

        except (AttributeError, KeyError) as e:
            logger.error(f"[ProactiveReply] [消息生成] 生成消息失败: {e}")
            return None

    async def _send_proactive_message(self, user_id: str, message: str) -> bool:
        """
        发送主动消息

        Args:
            user_id: 用户ID
            message: 消息内容

        Returns:
            是否发送成功
        """
        try:
            # 获取或创建该用户的发送锁，保证 check+record 原子性
            if user_id not in self._send_locks:
                self._send_locks[user_id] = asyncio.Lock()
            send_lock = self._send_locks[user_id]

            async with send_lock:
                # 检查频率限制
                freq_limit = self.config["qq_msg_frequency_limit"]
                recent_count = await self.db.get_recent_message_count(user_id, hours=MESSAGE_FREQUENCY_CHECK_HOURS)

                if recent_count >= freq_limit:
                    logger.warning(f"[ProactiveReply] [消息发送] 用户[{user_id[:16]}...]触发频率限制（{MESSAGE_FREQUENCY_CHECK_HOURS}小时内已发送{recent_count}条，限制{freq_limit}条），跳过发送")
                    return False

                # QQ戳一戳功能（仅aiocqhttp平台）
                if self.config["enable_qq_poke"]:
                    await self._send_qq_poke(user_id)

                # 构建并发送消息链（使用文档推荐的流式 API）
                message_chain = MessageChain().message(message)
                await self.context.send_message(user_id, message_chain)

                # 发送成功后再记录，避免发送失败占用频控额度
                await self.db.record_message_sent(user_id, "proactive")

            return True

        except (AttributeError, TypeError) as e:
            logger.error(f"[ProactiveReply] [消息发送] 发送消息失败: {e}")
            return False
        except Exception as e:
            logger.error(f"[ProactiveReply] [消息发送] 发送消息发生未知错误: {e}")
            return False

    async def _send_qq_poke(self, user_id: str):
        """
        发送QQ戳一戳

        Args:
            user_id: 用户ID
        """
        try:
            # 检查是否是QQ平台
            if not user_id.startswith("aiocqhttp_"):
                logger.debug(f"[ProactiveReply] [戳一戳] 用户[{user_id[:16]}...]不是aiocqhttp平台，跳过戳一戳")
                return

            # 解析QQ号
            # user_id格式: aiocqhttp_<bot_account>_private_<qq_number>
            parts = user_id.split("_")
            if len(parts) < 4:
                logger.warning(f"[ProactiveReply] [戳一戳] 无法解析用户ID格式: {user_id}")
                return

            qq_number = parts[-1]

            # 通过文档推荐的 platform_manager 获取 aiocqhttp 平台实例
            try:
                from astrbot.api.platform import AiocqhttpAdapter
                platforms = self.context.platform_manager.get_insts()
                aiocqhttp_platform = None
                for p in platforms:
                    if isinstance(p, AiocqhttpAdapter):
                        aiocqhttp_platform = p
                        break

                if not aiocqhttp_platform:
                    logger.debug("[ProactiveReply] [戳一戳] 未找到 aiocqhttp 平台实例，跳过戳一戳")
                    return

                # 通过文档推荐的 client.api.call_action 调用协议端 API
                client = aiocqhttp_platform.get_client()
                await client.api.call_action(
                    "send_private_poke",
                    user_id=int(qq_number)
                )

                logger.info(f"[ProactiveReply] [戳一戳] 已向用户[{user_id[:16]}...]发送戳一戳")

                # 戳一戳后等待一小段时间再发送消息
                await asyncio.sleep(1)

            except AttributeError:
                logger.debug("[ProactiveReply] [戳一戳] 平台不支持戳一戳功能")
            except (ValueError, TypeError) as e:
                logger.warning(f"[ProactiveReply] [戳一戳] 参数错误: {e}")
            except Exception as e:
                logger.warning(f"[ProactiveReply] [戳一戳] 发送戳一戳失败: {e}")

        except (IndexError, ValueError) as e:
            logger.error(f"[ProactiveReply] [戳一戳] 解析用户ID失败: {e}")

    async def _generate_daily_greeting_plan(self):
        """生成每日问候计划"""
        try:
            logger.info(f"[ProactiveReply] [定时任务] 开始生成当日日常问候计划，"
                       f"北京时间：{self.scheduler.get_beijing_time().strftime('%Y-%m-%d %H:%M')}")

            # 获取所有用户
            users = await self.db.get_all_users()
            time_ranges = self.config["greeting_time_ranges"]
            probabilities = self.config["greeting_probabilities"]

            for user_id in users:
                user_info = await self.db.get_user_info(user_id)
                if not user_info:
                    continue

                # 检查今日问候次数
                greeting_count = user_info.get('today_greeting_count', 0)
                max_count = self.config["daily_max_greeting_count"]

                if greeting_count >= max_count:
                    logger.debug(f"[ProactiveReply] [问候计划] 用户[{user_id[:16]}...]今日问候次数已达上限，跳过")
                    continue

                # 按时段生成问候任务
                generated_count = 0
                for idx, time_range in enumerate(time_ranges):
                    if generated_count >= max_count:
                        break

                    # 获取对应的概率（使用时段索引而不是生成计数）
                    prob = probabilities[idx] if idx < len(probabilities) else 0.0

                    # 概率判定
                    if random.random() > prob:
                        logger.debug(f"[ProactiveReply] [问候计划] 用户[{user_id[:16]}...]时段{idx + 1}问候概率未命中，跳过")
                        continue

                    # 在时段内随机选择一个时间
                    start_hour = time_range.get("start", 6)
                    end_hour = time_range.get("end", 8)

                    if start_hour == end_hour:
                        hour = start_hour
                        minute = random.randint(0, 59)
                    else:
                        hour = random.randint(start_hour, end_hour)
                        minute = random.randint(0, 59)

                    # 构建触发时间
                    now = self.scheduler.get_beijing_time()
                    trigger_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

                    # 如果时间已过，跳过
                    if trigger_time <= now:
                        continue

                    # 避开防打扰时段
                    disturb_start = self.config["disturb_free_start"]
                    disturb_end = self.config["disturb_free_end"]
                    trigger_time = self.scheduler.adjust_time_avoid_disturb(trigger_time, disturb_start, disturb_end)

                    # 生成任务ID
                    task_id = f"greeting_{user_id}_{uuid.uuid4().hex[:8]}"

                    # 保存任务，失败则跳过调度器注册
                    task_context = {
                        "time_range": time_range.get("name", ""),
                        "greeting_index": generated_count + 1
                    }
                    db_ok = await self.db.add_task(
                        task_id,
                        user_id,
                        int(trigger_time.timestamp()),
                        "greeting",
                        task_context
                    )
                    if not db_ok:
                        logger.error(f"[ProactiveReply] [问候计划] 任务[{task_id}]数据库保存失败，跳过调度器注册")
                        continue

                    # 注册到调度器，失败则将 DB 任务标记为取消
                    if not self.scheduler.add_one_time_task(
                        task_id,
                        trigger_time,
                        self._execute_greeting_task,
                        task_id
                    ):
                        logger.warning(f"[ProactiveReply] [问候计划] 任务[{task_id}]调度器注册失败，标记为取消")
                        await self.db.update_task_status(task_id, 3, "调度器注册失败")
                        continue

                    generated_count += 1
                    logger.info(f"[ProactiveReply] [问候计划] 为用户[{user_id[:16]}...]生成日常问候任务，"
                               f"触发时间：{trigger_time.strftime('%Y-%m-%d %H:%M')}（北京时间），"
                               f"当日已生成{generated_count}次")

            logger.info("[ProactiveReply] [定时任务] 每日问候计划生成完成")

        except (ValueError, KeyError, TypeError) as e:
            logger.error(f"[ProactiveReply] [定时任务] 生成每日问候计划失败: {e}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [定时任务] 生成每日问候计划发生未知错误: {e}")

    async def _generate_daily_events(self):
        """生成每日事件"""
        try:
            logger.info(f"[ProactiveReply] [定时任务] 开始生成当日日常事件，"
                       f"北京时间：{self.scheduler.get_beijing_time().strftime('%Y-%m-%d %H:%M')}")

            # 获取所有用户
            users = await self.db.get_all_users()
            max_event_count = self.config["daily_max_event_count"]

            async def process_user(user_id):
                """处理单个用户的事件生成"""
                async with self._llm_semaphore:
                    user_info = await self.db.get_user_info(user_id)
                    if not user_info:
                        return

                    # 检查今日事件次数
                    event_count = user_info.get('today_event_count', 0)
                    if event_count >= max_event_count:
                        logger.debug(f"[ProactiveReply] [事件生成] 用户[{user_id[:16]}...]今日事件次数已达上限，跳过")
                        return

                    # 生成事件（调用 LLM）
                    try:
                        provider_id = await self.context.get_current_chat_provider_id(umo=user_id)
                        now = self.scheduler.get_beijing_time()

                        prompt = f"""请基于角色人设，生成最多{max_event_count - event_count}个符合今天的日常事件。

当前日期：{now.strftime('%Y-%m-%d')}
当前时间：{now.strftime('%H:%M')}

每个事件需要包含：
1. 触发时间（格式：HH:MM，必须在今天剩余时间内，避开00:00-06:00）
2. 事件内容（简短描述，如"在奶茶店买了冰奶茶"）

请以JSON格式回复：
{{"events": [{{"time": "HH:MM", "content": "事件内容"}}]}}

注意：
- 事件时间必须晚于当前时间
- 两个事件间隔至少4小时
- 事件要符合角色人设和日常习惯"""

                        try:
                            llm_resp = await asyncio.wait_for(
                                self.context.llm_generate(
                                    chat_provider_id=provider_id,
                                    prompt=prompt
                                ),
                                timeout=LLM_TIMEOUT_SECONDS
                            )
                        except asyncio.TimeoutError:
                            logger.error(f"[ProactiveReply] [事件生成] 用户[{user_id[:16]}...]LLM调用超时（{LLM_TIMEOUT_SECONDS}秒）")
                            return

                        # 解析 JSON
                        json_str = self._extract_json_from_text(llm_resp.completion_text)
                        if not json_str:
                            logger.warning(f"[ProactiveReply] [事件生成] 用户[{user_id[:16]}...]事件生成失败：无法解析JSON")
                            return

                        try:
                            events_data = json.loads(json_str)
                        except json.JSONDecodeError as e:
                            logger.error(f"[ProactiveReply] [事件生成] 用户[{user_id[:16]}...]JSON解析失败: {e}")
                            return

                        # 验证JSON结构
                        if not isinstance(events_data, dict):
                            logger.error(f"[ProactiveReply] [事件生成] 用户[{user_id[:16]}...]JSON格式错误：不是字典类型")
                            return

                        events = events_data.get("events", [])
                        if not isinstance(events, list):
                            logger.error(f"[ProactiveReply] [事件生成] 用户[{user_id[:16]}...]events字段不是列表类型")
                            return

                        if not events:
                            logger.info(f"[ProactiveReply] [事件生成] 用户[{user_id[:16]}...]无事件生成")
                            return

                        # 单次遍历：校验间隔 → 防打扰调整 → 再校验 → 注册，直到达到上限
                        registered_times = []  # 已注册事件的最终触发时间（调整后）
                        remaining = max_event_count - event_count
                        disturb_start = self.config["disturb_free_start"]
                        disturb_end = self.config["disturb_free_end"]
                        min_interval_sec = MIN_EVENT_INTERVAL_HOURS * 3600

                        for event in events:
                            if remaining <= 0:
                                break
                            if not isinstance(event, dict):
                                logger.warning("[ProactiveReply] [事件生成] 事件格式错误，跳过")
                                continue

                            time_str = event.get("time", "")
                            content = event.get("content", "")

                            if not time_str or not content:
                                logger.warning("[ProactiveReply] [事件生成] 事件缺少必需字段，跳过")
                                continue

                            try:
                                hour, minute = map(int, time_str.split(':'))
                                trigger_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

                                if trigger_time <= now:
                                    continue

                                # 原始时间间隔校验
                                too_close = any(
                                    abs((trigger_time - prev).total_seconds()) < min_interval_sec
                                    for prev in registered_times
                                )
                                if too_close:
                                    logger.warning(f"[ProactiveReply] [事件生成] 用户[{user_id[:16]}...]事件{time_str}与已有事件间隔不足{MIN_EVENT_INTERVAL_HOURS}小时，跳过")
                                    continue

                                # 防打扰时段调整
                                trigger_time = self.scheduler.adjust_time_avoid_disturb(trigger_time, disturb_start, disturb_end)

                                # 调整后再次校验间隔（调整可能导致时间挤到一起）
                                too_close_after = any(
                                    abs((trigger_time - prev).total_seconds()) < min_interval_sec
                                    for prev in registered_times
                                )
                                if too_close_after:
                                    logger.warning(f"[ProactiveReply] [事件生成] 用户[{user_id[:16]}...]事件{time_str}防打扰调整后仍与已有事件间隔不足，跳过")
                                    continue

                                # 生成任务ID
                                task_id = f"event_{user_id}_{uuid.uuid4().hex[:8]}"

                                # 保存任务，失败则跳过调度器注册
                                task_context = {"event_content": content}
                                db_ok = await self.db.add_task(
                                    task_id,
                                    user_id,
                                    int(trigger_time.timestamp()),
                                    "event",
                                    task_context
                                )
                                if not db_ok:
                                    logger.error(f"[ProactiveReply] [事件生成] 任务[{task_id}]数据库保存失败，跳过调度器注册")
                                    continue

                                # 注册到调度器，失败则将 DB 任务标记为取消
                                if not self.scheduler.add_one_time_task(
                                    task_id,
                                    trigger_time,
                                    self._execute_event_task,
                                    task_id
                                ):
                                    logger.warning(f"[ProactiveReply] [事件生成] 任务[{task_id}]调度器注册失败，标记为取消")
                                    await self.db.update_task_status(task_id, 3, "调度器注册失败")
                                    continue

                                registered_times.append(trigger_time)
                                remaining -= 1
                                logger.info(f"[ProactiveReply] [事件生成] 为用户[{user_id[:16]}...]生成日常事件任务，"
                                           f"触发时间：{trigger_time.strftime('%Y-%m-%d %H:%M')}（北京时间），"
                                           f"事件内容：{content}")

                            except (ValueError, TypeError) as e:
                                logger.warning(f"[ProactiveReply] [事件生成] 解析事件时间失败: {e}")
                                continue

                    except asyncio.TimeoutError:
                        logger.error(f"[ProactiveReply] [事件生成] 用户[{user_id[:16]}...]LLM调用超时")
                    except (json.JSONDecodeError, KeyError, TypeError) as e:
                        logger.error(f"[ProactiveReply] [事件生成] 用户[{user_id[:16]}...]事件生成失败: {e}")

            # 分批并发处理，每批大小与信号量一致，避免一次性挂起大量协程
            batch_size = _LLM_CONCURRENCY
            for i in range(0, len(users), batch_size):
                batch = users[i:i + batch_size]
                results = await asyncio.gather(*[process_user(uid) for uid in batch], return_exceptions=True)
                for uid, result in zip(batch, results):
                    if isinstance(result, BaseException):
                        logger.error(f"[ProactiveReply] [事件生成] 用户[{uid[:16]}...]处理异常: {result}")

            logger.info("[ProactiveReply] [定时任务] 每日事件生成完成")

        except (ValueError, KeyError) as e:
            logger.error(f"[ProactiveReply] [定时任务] 生成每日事件失败: {e}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [定时任务] 生成每日事件发生未知错误: {e}")

    async def _scan_inactive_users(self):
        """扫描未活跃用户"""
        try:
            logger.info(f"[ProactiveReply] [定时任务] 开始扫描未活跃用户，"
                       f"北京时间：{self.scheduler.get_beijing_time().strftime('%Y-%m-%d %H:%M')}")

            now = self.scheduler.get_beijing_time()
            users = await self.db.get_all_users()

            async def process_user(user_id):
                """处理单个用户的未活跃检查"""
                async with self._llm_semaphore:
                    user_info = await self.db.get_user_info(user_id)
                    if not user_info:
                        return

                    last_active = datetime.fromtimestamp(user_info['last_active_time'], self.scheduler.beijing_tz)
                    hours_inactive = (now - last_active).total_seconds() / 3600

                    # 1天未活跃触达（24小时）
                    if hours_inactive >= INACTIVE_1D_HOURS and hours_inactive < INACTIVE_6D_HOURS:
                        if self.config["enable_inactive_1d_touch"] and user_info['last_1d_inactive_touch_time'] == 0:
                            logger.info(f"[ProactiveReply] [未活跃触达] 用户[{user_id[:16]}...]已{int(hours_inactive/24)}天未活跃，触发梯度1触达")

                            try:
                                provider_id = await self.context.get_current_chat_provider_id(umo=user_id)
                                persona_mgr = self.context.persona_manager
                                persona = await persona_mgr.get_default_persona_v3(umo=user_id)
                                persona_prompt = persona.prompt if persona and hasattr(persona, 'prompt') else ""

                                prompt = f"""请基于角色人设，生成一条自然的问候/关心消息，因为用户已经1天没有和你聊天了。

人设：{persona_prompt}

要求：
- 保持人设不变
- 语气自然、关心
- 不要太长，1-2句话即可
- 不要提及"1天"这个具体时间

直接输出消息内容，不要有其他说明。"""

                                try:
                                    llm_resp = await asyncio.wait_for(
                                        self.context.llm_generate(
                                            chat_provider_id=provider_id,
                                            prompt=prompt
                                        ),
                                        timeout=LLM_TIMEOUT_SECONDS
                                    )
                                except asyncio.TimeoutError:
                                    logger.error(f"[ProactiveReply] [未活跃触达] 用户[{user_id[:16]}...]LLM调用超时（{LLM_TIMEOUT_SECONDS}秒）")
                                    return

                                message = llm_resp.completion_text.strip()
                                success = await self._send_proactive_message(user_id, message)

                                if success:
                                    await self.db.update_inactive_touch_time(user_id, "1d")
                                    logger.info(f"[ProactiveReply] [消息发送] 用户[{user_id[:16]}...]的未活跃触达消息发送成功，梯度：1")

                            except asyncio.TimeoutError:
                                logger.error(f"[ProactiveReply] [未活跃触达] 用户[{user_id[:16]}...]LLM调用超时")
                            except (AttributeError, TypeError) as e:
                                logger.error(f"[ProactiveReply] [未活跃触达] 用户[{user_id[:16]}...]梯度1触达失败: {e}")

                    # 6天未活跃触达（144小时）
                    elif hours_inactive >= INACTIVE_6D_HOURS:
                        if self.config["enable_inactive_6d_touch"] and user_info['last_6d_inactive_touch_time'] == 0:
                            logger.info(f"[ProactiveReply] [未活跃触达] 用户[{user_id[:16]}...]已{int(hours_inactive/24)}天未活跃，触发梯度2触达")

                            try:
                                provider_id = await self.context.get_current_chat_provider_id(umo=user_id)
                                persona_mgr = self.context.persona_manager
                                persona = await persona_mgr.get_default_persona_v3(umo=user_id)
                                persona_prompt = persona.prompt if persona and hasattr(persona, 'prompt') else ""

                                prompt = f"""请基于角色人设，生成一条带点生气、质问语气的消息，因为用户已经超过6天没有和你聊天了。

人设：{persona_prompt}

要求：
- 保持人设不变
- 语气带点生气、质问，但不要太过分
- 可以表达想念、不满等情绪
- 1-2句话即可

直接输出消息内容，不要有其他说明。"""

                                try:
                                    llm_resp = await asyncio.wait_for(
                                        self.context.llm_generate(
                                            chat_provider_id=provider_id,
                                            prompt=prompt
                                        ),
                                        timeout=LLM_TIMEOUT_SECONDS
                                    )
                                except asyncio.TimeoutError:
                                    logger.error(f"[ProactiveReply] [未活跃触达] 用户[{user_id[:16]}...]LLM调用超时（{LLM_TIMEOUT_SECONDS}秒）")
                                    return

                                message = llm_resp.completion_text.strip()
                                success = await self._send_proactive_message(user_id, message)

                                if success:
                                    await self.db.update_inactive_touch_time(user_id, "6d")
                                    logger.info(f"[ProactiveReply] [消息发送] 用户[{user_id[:16]}...]的未活跃触达消息发送成功，梯度：2")

                            except asyncio.TimeoutError:
                                logger.error(f"[ProactiveReply] [未活跃触达] 用户[{user_id[:16]}...]LLM调用超时")
                            except (AttributeError, TypeError) as e:
                                logger.error(f"[ProactiveReply] [未活跃触达] 用户[{user_id[:16]}...]梯度2触达失败: {e}")

            # 分批并发处理，每批大小与信号量一致，避免一次性挂起大量协程
            batch_size = _LLM_CONCURRENCY
            for i in range(0, len(users), batch_size):
                batch = users[i:i + batch_size]
                results = await asyncio.gather(*[process_user(uid) for uid in batch], return_exceptions=True)
                for uid, result in zip(batch, results):
                    if isinstance(result, BaseException):
                        logger.error(f"[ProactiveReply] [未活跃触达] 用户[{uid[:16]}...]处理异常: {result}")

            logger.info("[ProactiveReply] [定时任务] 未活跃用户扫描完成")

        except (ValueError, KeyError, TypeError) as e:
            logger.error(f"[ProactiveReply] [定时任务] 扫描未活跃用户失败: {e}")

    async def _execute_greeting_task(self, task_id: str):
        """
        执行问候任务

        Args:
            task_id: 任务ID
        """
        try:
            # 获取任务信息（直接通过 task_id 查询）
            tasks = await self.db.get_pending_tasks(task_id=task_id)

            if not tasks:
                logger.warning(f"[ProactiveReply] [任务执行] 问候任务[{task_id}]不存在")
                return

            task = tasks[0]

            logger.info(f"[ProactiveReply] [任务执行] 开始执行用户[{task['user_id'][:16]}...]的问候任务[{task_id}]")

            user_id = task['user_id']
            task_context = task.get('task_context', {})

            # 生成问候消息
            try:
                provider_id = await self.context.get_current_chat_provider_id(umo=user_id)
                now = self.scheduler.get_beijing_time()
                time_range = task_context.get('time_range', '')

                # 获取人设
                persona_mgr = self.context.persona_manager
                persona = await persona_mgr.get_default_persona_v3(umo=user_id)
                persona_prompt = persona.prompt if persona and hasattr(persona, 'prompt') else ""

                prompt = f"""请基于角色人设，生成一条{time_range}的日常问候消息。

人设：{persona_prompt}

当前时间：{now.strftime('%H:%M')}

要求：
- 保持人设不变
- 符合当前时段的特点
- 自然、简短，1-2句话
- 不要太正式

直接输出消息内容，不要有其他说明。"""

                try:
                    llm_resp = None
                    max_retries = 2
                    for attempt in range(max_retries + 1):
                        try:
                            llm_resp = await asyncio.wait_for(
                                self.context.llm_generate(
                                    chat_provider_id=provider_id,
                                    prompt=prompt
                                ),
                                timeout=LLM_TIMEOUT_SECONDS
                            )
                            break
                        except asyncio.TimeoutError:
                            logger.warning(f"[ProactiveReply] [任务执行] 问候任务LLM调用超时（第{attempt + 1}次）")
                            if attempt < max_retries:
                                await asyncio.sleep(2)
                        except Exception as llm_err:
                            logger.warning(f"[ProactiveReply] [任务执行] 问候任务LLM调用失败（第{attempt + 1}次）: {llm_err}")
                            if attempt < max_retries:
                                await asyncio.sleep(2)

                    if not llm_resp:
                        logger.error(f"[ProactiveReply] [任务执行] 问候任务LLM调用失败，已重试{max_retries}次，放弃")
                        await self.db.update_task_status(task_id, 2, "LLM调用失败")
                        return

                    message = llm_resp.completion_text.strip()

                    # 发送消息
                    success = await self._send_proactive_message(user_id, message)

                    if success:
                        await self.db.update_task_status(task_id, 1)
                        await self.db.increment_greeting_count(user_id)
                        logger.info(f"[ProactiveReply] [消息发送] 用户[{user_id[:16]}...]的问候任务执行成功")
                    else:
                        await self.db.update_task_status(task_id, 2, "消息发送失败")

                except (AttributeError, KeyError) as e:
                    logger.error(f"[ProactiveReply] [任务执行] 问候任务执行失败: {e}")
                    await self.db.update_task_status(task_id, 2, str(e))

            except (KeyError, TypeError) as e:
                logger.error(f"[ProactiveReply] [任务执行] 执行问候任务失败: {e}")

        except Exception as e:
            logger.error(f"[ProactiveReply] [任务执行] 执行问候任务发生未知错误: {e}")

    async def _execute_event_task(self, task_id: str):
        """
        执行事件任务

        Args:
            task_id: 任务ID
        """
        try:
            # 获取任务信息（直接通过 task_id 查询）
            tasks = await self.db.get_pending_tasks(task_id=task_id)

            if not tasks:
                logger.warning(f"[ProactiveReply] [任务执行] 事件任务[{task_id}]不存在")
                return

            task = tasks[0]

            logger.info(f"[ProactiveReply] [任务执行] 开始执行用户[{task['user_id'][:16]}...]的事件任务[{task_id}]")

            user_id = task['user_id']
            task_context = task.get('task_context', {})
            event_content = task_context.get('event_content', '')

            # 生成事件消息
            try:
                provider_id = await self.context.get_current_chat_provider_id(umo=user_id)

                # 获取人设
                persona_mgr = self.context.persona_manager
                persona = await persona_mgr.get_default_persona_v3(umo=user_id)
                persona_prompt = persona.prompt if persona and hasattr(persona, 'prompt') else ""

                prompt = f"""请基于角色人设，以这件事为话题，生成一条自然的主动消息发给用户。

人设：{persona_prompt}

事件内容：{event_content}

要求：
- 保持人设不变
- 你现在正在做这件事
- 以这件事为话题，自然地分享给用户
- 1-2句话即可

直接输出消息内容，不要有其他说明。"""

                try:
                    llm_resp = None
                    max_retries = 2
                    for attempt in range(max_retries + 1):
                        try:
                            llm_resp = await asyncio.wait_for(
                                self.context.llm_generate(
                                    chat_provider_id=provider_id,
                                    prompt=prompt
                                ),
                                timeout=LLM_TIMEOUT_SECONDS
                            )
                            break
                        except asyncio.TimeoutError:
                            logger.warning(f"[ProactiveReply] [任务执行] 事件任务LLM调用超时（第{attempt + 1}次）")
                            if attempt < max_retries:
                                await asyncio.sleep(2)
                        except Exception as llm_err:
                            logger.warning(f"[ProactiveReply] [任务执行] 事件任务LLM调用失败（第{attempt + 1}次）: {llm_err}")
                            if attempt < max_retries:
                                await asyncio.sleep(2)

                    if not llm_resp:
                        logger.error(f"[ProactiveReply] [任务执行] 事件任务LLM调用失败，已重试{max_retries}次，放弃")
                        await self.db.update_task_status(task_id, 2, "LLM调用失败")
                        return

                    message = llm_resp.completion_text.strip()

                    # 发送消息
                    success = await self._send_proactive_message(user_id, message)

                    if success:
                        await self.db.update_task_status(task_id, 1)
                        await self.db.increment_event_count(user_id)
                        logger.info(f"[ProactiveReply] [消息发送] 用户[{user_id[:16]}...]的事件任务执行成功")
                    else:
                        await self.db.update_task_status(task_id, 2, "消息发送失败")

                except (AttributeError, KeyError) as e:
                    logger.error(f"[ProactiveReply] [任务执行] 事件任务执行失败: {e}")
                    await self.db.update_task_status(task_id, 2, str(e))

            except (KeyError, TypeError) as e:
                logger.error(f"[ProactiveReply] [任务执行] 执行事件任务失败: {e}")

        except Exception as e:
            logger.error(f"[ProactiveReply] [任务执行] 执行事件任务发生未知错误: {e}")

    async def terminate(self):
        """插件卸载时调用"""
        try:
            logger.info("[ProactiveReply] [插件卸载] 开始卸载插件")

            # 取消初始化任务（若仍在运行）
            if hasattr(self, '_init_task') and not self._init_task.done():
                self._init_task.cancel()
                try:
                    await self._init_task
                except asyncio.CancelledError:
                    pass

            # 取消所有对话倒计时任务
            for user_id, task in list(self.conversation_timers.items()):
                if not task.done():
                    task.cancel()
                    logger.debug(f"[ProactiveReply] [插件卸载] 取消用户[{user_id[:16]}...]的对话倒计时任务")
            self.conversation_timers.clear()

            # 关闭调度器（非阻塞，避免在事件循环中同步等待任务完成）
            if self.scheduler.is_running:
                logger.info("[ProactiveReply] [插件卸载] 正在关闭调度器...")
                await self.scheduler.shutdown(wait=False)

            # 关闭数据库
            await self.db.close()

            logger.info("[ProactiveReply] [插件卸载] 插件卸载完成")
        except (RuntimeError, AttributeError) as e:
            logger.error(f"[ProactiveReply] [插件卸载] 插件卸载失败: {e}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [插件卸载] 插件卸载发生未知错误: {e}")