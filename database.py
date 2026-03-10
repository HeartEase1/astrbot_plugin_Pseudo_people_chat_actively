"""
数据持久化模块
负责管理用户活跃状态、待执行任务、日常事件等数据的存储
使用 asyncio.to_thread 避免阻塞事件循环
"""
import sqlite3
import json
import asyncio
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path
import pytz
from astrbot.api import logger


class DatabaseManager:
    """数据库管理器"""

    # 数据库版本号
    DB_VERSION = 1

    def __init__(self, db_path: Path):
        """
        初始化数据库管理器

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.beijing_tz = pytz.timezone('Asia/Shanghai')

    async def initialize(self):
        """初始化数据库连接和表结构"""
        try:
            logger.info(f"[ProactiveReply] [数据库] 初始化数据库，路径: {self.db_path}")

            # 确保目录存在
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            # 连接数据库
            self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self.conn.row_factory = sqlite3.Row

            # 创建表
            await self._create_tables()

            # 执行数据库迁移
            await self._migrate_database()

            logger.info("[ProactiveReply] [数据库] 数据库初始化完成")
        except sqlite3.Error as e:
            logger.error(f"[ProactiveReply] [数据库] 数据库初始化失败: {e}")
            raise
        except OSError as e:
            logger.error(f"[ProactiveReply] [数据库] 文件系统错误: {e}")
            raise
        except Exception as e:
            logger.error(f"[ProactiveReply] [数据库] 数据库初始化发生未知错误: {e}")
            raise

    async def _create_tables(self):
        """创建数据库表"""
        cursor = self.conn.cursor()

        # 用户基础信息表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_base_info (
                user_id TEXT PRIMARY KEY,
                last_active_time INTEGER NOT NULL,
                last_1d_inactive_touch_time INTEGER DEFAULT 0,
                last_6d_inactive_touch_time INTEGER DEFAULT 0,
                today_greeting_count INTEGER DEFAULT 0,
                today_event_count INTEGER DEFAULT 0,
                create_time INTEGER NOT NULL,
                update_time INTEGER NOT NULL
            )
        """)

        # 待执行任务表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pending_tasks (
                task_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                trigger_time INTEGER NOT NULL,
                task_type TEXT NOT NULL,
                task_context TEXT,
                create_time INTEGER NOT NULL,
                status INTEGER DEFAULT 0,
                exec_time INTEGER DEFAULT 0,
                fail_reason TEXT
            )
        """)

        # 日常事件表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_events (
                event_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                event_date TEXT NOT NULL,
                trigger_time INTEGER NOT NULL,
                weather TEXT,
                event_content TEXT NOT NULL,
                status INTEGER DEFAULT 0,
                create_time INTEGER NOT NULL
            )
        """)

        # 消息发送记录表（用于频率限制）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS message_send_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                send_time INTEGER NOT NULL,
                message_type TEXT,
                create_time INTEGER NOT NULL
            )
        """)

        # 创建索引以提高查询效率
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_send_records_user_time
            ON message_send_records(user_id, send_time)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pending_tasks_user_id
            ON pending_tasks(user_id)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pending_tasks_task_id
            ON pending_tasks(task_id)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pending_tasks_status
            ON pending_tasks(status, trigger_time)
        """)

        self.conn.commit()
        logger.info("[ProactiveReply] [数据库] 数据表创建完成")

    async def _get_db_version(self) -> int:
        """获取当前数据库版本号"""
        def _get():
            try:
                cursor = self.conn.cursor()
                # 检查版本表是否存在
                cursor.execute("""
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name='db_version'
                """)
                if not cursor.fetchone():
                    # 版本表不存在，创建并初始化为版本0
                    cursor.execute("""
                        CREATE TABLE db_version (
                            version INTEGER PRIMARY KEY
                        )
                    """)
                    cursor.execute("INSERT INTO db_version (version) VALUES (0)")
                    self.conn.commit()
                    return 0

                # 获取当前版本
                cursor.execute("SELECT version FROM db_version")
                row = cursor.fetchone()
                return row[0] if row else 0
            except sqlite3.Error as e:
                logger.error(f"[ProactiveReply] [数据库迁移] 获取数据库版本失败: {e}")
                return 0
            except Exception as e:
                logger.error(f"[ProactiveReply] [数据库迁移] 获取数据库版本发生未知错误: {e}")
                return 0

        return await asyncio.to_thread(_get)

    async def _set_db_version(self, version: int):
        """设置数据库版本号"""
        def _set():
            try:
                cursor = self.conn.cursor()
                cursor.execute("UPDATE db_version SET version = ?", (version,))
                self.conn.commit()
                logger.info(f"[ProactiveReply] [数据库迁移] 数据库版本已更新至 v{version}")
            except sqlite3.Error as e:
                logger.error(f"[ProactiveReply] [数据库迁移] 设置数据库版本失败: {e}")
            except Exception as e:
                logger.error(f"[ProactiveReply] [数据库迁移] 设置数据库版本发生未知错误: {e}")

        await asyncio.to_thread(_set)

    async def _migrate_database(self):
        """执行数据库迁移"""
        try:
            current_version = await self._get_db_version()
            target_version = self.DB_VERSION

            if current_version == target_version:
                logger.info(f"[ProactiveReply] [数据库迁移] 数据库版本已是最新 (v{current_version})")
                return

            if current_version > target_version:
                logger.warning(f"[ProactiveReply] [数据库迁移] 数据库版本 (v{current_version}) 高于代码版本 (v{target_version})，跳过迁移")
                return

            logger.info(f"[ProactiveReply] [数据库迁移] 开始数据库迁移: v{current_version} -> v{target_version}")

            # 执行迁移脚本
            for version in range(current_version + 1, target_version + 1):
                migration_method = getattr(self, f'_migrate_to_v{version}', None)
                if migration_method:
                    logger.info(f"[ProactiveReply] [数据库迁移] 执行迁移: v{version - 1} -> v{version}")
                    await migration_method()
                    await self._set_db_version(version)
                else:
                    logger.warning(f"[ProactiveReply] [数据库迁移] 未找到迁移方法: _migrate_to_v{version}")

            logger.info(f"[ProactiveReply] [数据库迁移] 数据库迁移完成: v{current_version} -> v{target_version}")

        except sqlite3.Error as e:
            logger.error(f"[ProactiveReply] [数据库迁移] 数据库迁移失败: {e}")
            raise
        except Exception as e:
            logger.error(f"[ProactiveReply] [数据库迁移] 数据库迁移发生未知错误: {e}")
            raise

    # 迁移方法示例（未来版本使用）
    # async def _migrate_to_v2(self):
    #     """迁移到版本2的示例"""
    #     def _migrate():
    #         try:
    #             cursor = self.conn.cursor()
    #             # 示例：添加新字段
    #             cursor.execute("""
    #                 ALTER TABLE user_base_info
    #                 ADD COLUMN new_field TEXT DEFAULT ''
    #             """)
    #             self.conn.commit()
    #             logger.info("[ProactiveReply] [数据库迁移] v2迁移完成：添加新字段")
    #         except sqlite3.Error as e:
    #             logger.error(f"[ProactiveReply] [数据库迁移] v2迁移失败: {e}")
    #             raise
    #
    #     await asyncio.to_thread(_migrate)

    def _get_beijing_timestamp(self) -> int:
        """获取北京时间时间戳"""
        return int(datetime.now(self.beijing_tz).timestamp())

    async def update_user_active_time(self, user_id: str):
        """
        更新用户活跃时间

        Args:
            user_id: 用户ID
        """
        def _update():
            try:
                cursor = self.conn.cursor()
                now = self._get_beijing_timestamp()

                # 检查用户是否存在
                cursor.execute("SELECT user_id FROM user_base_info WHERE user_id = ?", (user_id,))
                exists = cursor.fetchone()

                if exists:
                    # 更新活跃时间，并重置未活跃触达记录
                    cursor.execute("""
                        UPDATE user_base_info
                        SET last_active_time = ?,
                            last_1d_inactive_touch_time = 0,
                            last_6d_inactive_touch_time = 0,
                            update_time = ?
                        WHERE user_id = ?
                    """, (now, now, user_id))
                    logger.info(f"[ProactiveReply] [用户状态] 用户[{user_id[:8]}...]已活跃，重置未活跃触达记录")
                else:
                    # 新建用户记录
                    cursor.execute("""
                        INSERT INTO user_base_info
                        (user_id, last_active_time, create_time, update_time)
                        VALUES (?, ?, ?, ?)
                    """, (user_id, now, now, now))
                    logger.info(f"[ProactiveReply] [用户状态] 新用户[{user_id[:8]}...]记录创建")

                self.conn.commit()
            except sqlite3.Error as e:
                self.conn.rollback()
                logger.error(f"[ProactiveReply] [数据库] 更新用户活跃时间失败: {e}")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"[ProactiveReply] [数据库] 更新用户活跃时间发生未知错误: {e}")

        await asyncio.to_thread(_update)

    async def get_user_info(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        获取用户信息

        Args:
            user_id: 用户ID

        Returns:
            用户信息字典，不存在返回None
        """
        def _get():
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT * FROM user_base_info WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()

                if row:
                    return dict(row)
                return None
            except sqlite3.Error as e:
                logger.error(f"[ProactiveReply] [数据库] 获取用户信息失败: {e}")
                return None
            except Exception as e:
                logger.error(f"[ProactiveReply] [数据库] 获取用户信息发生未知错误: {e}")
                return None

        return await asyncio.to_thread(_get)

    async def add_task(self, task_id: str, user_id: str, trigger_time: int,
                      task_type: str, task_context: Dict[str, Any]):
        """
        添加待执行任务

        Args:
            task_id: 任务ID
            user_id: 用户ID
            trigger_time: 触发时间戳（北京时间）
            task_type: 任务类型
            task_context: 任务上下文
        """
        def _add():
            try:
                # 检查数据库连接
                if not self.conn:
                    logger.warning(f"[ProactiveReply] [数据库] 数据库连接已关闭，跳过添加任务")
                    return

                cursor = self.conn.cursor()
                now = self._get_beijing_timestamp()

                cursor.execute("""
                    INSERT INTO pending_tasks
                    (task_id, user_id, trigger_time, task_type, task_context, create_time, status)
                    VALUES (?, ?, ?, ?, ?, ?, 0)
                """, (task_id, user_id, trigger_time, task_type, json.dumps(task_context, ensure_ascii=False), now))

                self.conn.commit()

                # 转换时间戳为北京时间字符串
                trigger_dt = datetime.fromtimestamp(trigger_time, self.beijing_tz)
                logger.info(f"[ProactiveReply] [任务注册] 用户[{user_id[:8]}...]任务注册成功，任务ID：{task_id}，"
                           f"触发时间：{trigger_dt.strftime('%Y-%m-%d %H:%M')}（北京时间）")
            except sqlite3.Error as e:
                self.conn.rollback()
                logger.error(f"[ProactiveReply] [数据库] 添加任务失败: {e}")
            except (ValueError, TypeError) as e:
                self.conn.rollback()
                logger.error(f"[ProactiveReply] [数据库] 任务数据格式错误: {e}")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"[ProactiveReply] [数据库] 添加任务发生未知错误: {e}")

        await asyncio.to_thread(_add)

    async def get_pending_tasks(self, user_id: Optional[str] = None, task_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取待执行任务

        Args:
            user_id: 用户ID，为None时获取所有用户的任务
            task_id: 任务ID，为None时获取所有任务

        Returns:
            任务列表
        """
        def _get():
            try:
                # 检查数据库连接
                if not self.conn:
                    logger.warning(f"[ProactiveReply] [数据库] 数据库连接已关闭，返回空任务列表")
                    return []

                cursor = self.conn.cursor()

                if task_id:
                    # 直接通过 task_id 查询
                    cursor.execute("""
                        SELECT * FROM pending_tasks
                        WHERE task_id = ? AND status = 0
                    """, (task_id,))
                elif user_id:
                    cursor.execute("""
                        SELECT * FROM pending_tasks
                        WHERE user_id = ? AND status = 0
                        ORDER BY trigger_time ASC
                    """, (user_id,))
                else:
                    cursor.execute("""
                        SELECT * FROM pending_tasks
                        WHERE status = 0
                        ORDER BY trigger_time ASC
                    """)

                rows = cursor.fetchall()
                tasks = []
                for row in rows:
                    task = dict(row)
                    if task['task_context']:
                        task['task_context'] = json.loads(task['task_context'])
                    tasks.append(task)

                return tasks
            except sqlite3.Error as e:
                logger.error(f"[ProactiveReply] [数据库] 获取待执行任务失败: {e}")
                return []
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"[ProactiveReply] [数据库] 任务数据解析失败: {e}")
                return []
            except Exception as e:
                logger.error(f"[ProactiveReply] [数据库] 获取待执行任务发生未知错误: {e}")
                return []

        return await asyncio.to_thread(_get)

    async def update_task_status(self, task_id: str, status: int, fail_reason: str = ""):
        """
        更新任务状态

        Args:
            task_id: 任务ID
            status: 状态（0-待执行、1-已执行、2-执行失败、3-已取消）
            fail_reason: 失败原因
        """
        def _update():
            try:
                # 检查数据库连接
                if not self.conn:
                    logger.warning(f"[ProactiveReply] [数据库] 数据库连接已关闭，跳过更新任务状态")
                    return

                cursor = self.conn.cursor()
                now = self._get_beijing_timestamp()

                cursor.execute("""
                    UPDATE pending_tasks
                    SET status = ?, exec_time = ?, fail_reason = ?
                    WHERE task_id = ?
                """, (status, now, fail_reason, task_id))

                self.conn.commit()

                status_text = {0: "待执行", 1: "已执行", 2: "执行失败", 3: "已取消"}
                logger.info(f"[ProactiveReply] [任务状态] 任务[{task_id}]状态更新为：{status_text.get(status, '未知')}")
            except sqlite3.Error as e:
                self.conn.rollback()
                logger.error(f"[ProactiveReply] [数据库] 更新任务状态失败: {e}")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"[ProactiveReply] [数据库] 更新任务状态发生未知错误: {e}")

        await asyncio.to_thread(_update)

    async def reset_daily_counters(self):
        """重置每日计数器（问候次数、事件次数）"""
        def _reset():
            try:
                cursor = self.conn.cursor()
                cursor.execute("""
                    UPDATE user_base_info
                    SET today_greeting_count = 0, today_event_count = 0
                """)
                self.conn.commit()
                logger.info("[ProactiveReply] [数据库] 每日计数器已重置")
            except sqlite3.Error as e:
                logger.error(f"[ProactiveReply] [数据库] 重置每日计数器失败: {e}")
            except Exception as e:
                logger.error(f"[ProactiveReply] [数据库] 重置每日计数器发生未知错误: {e}")

        await asyncio.to_thread(_reset)

    async def increment_greeting_count(self, user_id: str):
        """增加用户问候次数"""
        def _increment():
            try:
                cursor = self.conn.cursor()
                cursor.execute("""
                    UPDATE user_base_info
                    SET today_greeting_count = today_greeting_count + 1
                    WHERE user_id = ?
                """, (user_id,))
                self.conn.commit()
            except sqlite3.Error as e:
                logger.error(f"[ProactiveReply] [数据库] 增加问候次数失败: {e}")
            except Exception as e:
                logger.error(f"[ProactiveReply] [数据库] 增加问候次数发生未知错误: {e}")

        await asyncio.to_thread(_increment)

    async def increment_event_count(self, user_id: str):
        """增加用户事件次数"""
        def _increment():
            try:
                cursor = self.conn.cursor()
                cursor.execute("""
                    UPDATE user_base_info
                    SET today_event_count = today_event_count + 1
                    WHERE user_id = ?
                """, (user_id,))
                self.conn.commit()
            except sqlite3.Error as e:
                logger.error(f"[ProactiveReply] [数据库] 增加事件次数失败: {e}")
            except Exception as e:
                logger.error(f"[ProactiveReply] [数据库] 增加事件次数发生未知错误: {e}")

        await asyncio.to_thread(_increment)

    async def get_all_users(self) -> List[str]:
        """获取所有用户ID列表"""
        def _get():
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT user_id FROM user_base_info")
                rows = cursor.fetchall()
                return [row['user_id'] for row in rows]
            except sqlite3.Error as e:
                logger.error(f"[ProactiveReply] [数据库] 获取所有用户失败: {e}")
                return []
            except Exception as e:
                logger.error(f"[ProactiveReply] [数据库] 获取所有用户发生未知错误: {e}")
                return []

        return await asyncio.to_thread(_get)

    async def get_recent_message_count(self, user_id: str, hours: int) -> int:
        """
        获取最近N小时内的消息发送数量

        Args:
            user_id: 用户ID
            hours: 小时数

        Returns:
            消息数量
        """
        def _get():
            try:
                cursor = self.conn.cursor()
                now = self._get_beijing_timestamp()
                time_threshold = now - (hours * 3600)

                cursor.execute("""
                    SELECT COUNT(*) as count FROM message_send_records
                    WHERE user_id = ? AND send_time >= ?
                """, (user_id, time_threshold))

                row = cursor.fetchone()
                return row['count'] if row else 0
            except sqlite3.Error as e:
                logger.error(f"[ProactiveReply] [数据库] 获取消息发送记录失败: {e}")
                return 0
            except Exception as e:
                logger.error(f"[ProactiveReply] [数据库] 获取消息发送记录发生未知错误: {e}")
                return 0

        return await asyncio.to_thread(_get)

    async def record_message_sent(self, user_id: str, message_type: str = ""):
        """
        记录消息发送

        Args:
            user_id: 用户ID
            message_type: 消息类型
        """
        def _record():
            try:
                cursor = self.conn.cursor()
                now = self._get_beijing_timestamp()

                cursor.execute("""
                    INSERT INTO message_send_records
                    (user_id, send_time, message_type, create_time)
                    VALUES (?, ?, ?, ?)
                """, (user_id, now, message_type, now))

                self.conn.commit()
            except sqlite3.Error as e:
                logger.error(f"[ProactiveReply] [数据库] 记录消息发送失败: {e}")
            except Exception as e:
                logger.error(f"[ProactiveReply] [数据库] 记录消息发送发生未知错误: {e}")

        await asyncio.to_thread(_record)

    async def cleanup_old_send_records(self, days: int = 7):
        """
        清理旧的消息发送记录

        Args:
            days: 保留最近N天的记录
        """
        def _cleanup():
            try:
                cursor = self.conn.cursor()
                now = self._get_beijing_timestamp()
                time_threshold = now - (days * 24 * 3600)

                cursor.execute("""
                    DELETE FROM message_send_records
                    WHERE send_time < ?
                """, (time_threshold,))

                self.conn.commit()
                deleted_count = cursor.rowcount
                logger.info(f"[ProactiveReply] [数据库] 清理了{deleted_count}条旧消息记录")
            except sqlite3.Error as e:
                logger.error(f"[ProactiveReply] [数据库] 清理旧消息记录失败: {e}")
            except Exception as e:
                logger.error(f"[ProactiveReply] [数据库] 清理旧消息记录发生未知错误: {e}")

        await asyncio.to_thread(_cleanup)

    async def update_inactive_touch_time(self, user_id: str, touch_type: str):
        """
        更新未活跃触达时间

        Args:
            user_id: 用户ID
            touch_type: 触达类型（"1d" 或 "6d"）
        """
        def _update():
            try:
                cursor = self.conn.cursor()
                now = self._get_beijing_timestamp()

                if touch_type == "1d":
                    cursor.execute("""
                        UPDATE user_base_info
                        SET last_1d_inactive_touch_time = ?
                        WHERE user_id = ?
                    """, (now, user_id))
                elif touch_type == "6d":
                    cursor.execute("""
                        UPDATE user_base_info
                        SET last_6d_inactive_touch_time = ?
                        WHERE user_id = ?
                    """, (now, user_id))

                self.conn.commit()
                logger.info(f"[ProactiveReply] [数据库] 用户[{user_id[:8]}...]的{touch_type}触达时间已更新")
            except sqlite3.Error as e:
                logger.error(f"[ProactiveReply] [数据库] 更新触达时间失败: {e}")
            except Exception as e:
                logger.error(f"[ProactiveReply] [数据库] 更新触达时间发生未知错误: {e}")

        await asyncio.to_thread(_update)

    async def close(self):
        """关闭数据库连接"""
        def _close():
            if self.conn:
                self.conn.close()
                logger.info("[ProactiveReply] [数据库] 数据库连接已关闭")

        if self.conn:
            await asyncio.to_thread(_close)
            self.conn = None
