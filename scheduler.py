"""
定时任务调度模块
负责管理所有主动回复任务的注册、触发、暂停、恢复
强制锁定北京时间时区
"""
import asyncio
from datetime import datetime, timedelta
from typing import Callable, Optional
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
from astrbot.api import logger


class TaskScheduler:
    """任务调度器"""

    def __init__(self):
        """初始化调度器"""
        self.beijing_tz = pytz.timezone('Asia/Shanghai')
        self.scheduler = AsyncIOScheduler(timezone=self.beijing_tz)
        self.is_running = False

    async def start(self):
        """启动调度器"""
        try:
            if not self.is_running:
                self.scheduler.start()
                self.is_running = True
                logger.info("[ProactiveReply] [定时任务] 调度器启动成功，时区：Asia/Shanghai（北京时间）")
        except RuntimeError as e:
            logger.error(f"[ProactiveReply] [定时任务] 调度器启动失败: {e}")
            raise
        except Exception as e:
            logger.error(f"[ProactiveReply] [定时任务] 调度器启动发生未知错误: {e}")
            raise

    async def shutdown(self):
        """关闭调度器"""
        try:
            if self.is_running:
                self.scheduler.shutdown(wait=False)
                self.is_running = False
                logger.info("[ProactiveReply] [定时任务] 调度器已关闭")
        except RuntimeError as e:
            logger.error(f"[ProactiveReply] [定时任务] 调度器关闭失败: {e}")
        except Exception as e:
            logger.error(f"[ProactiveReply] [定时任务] 调度器关闭发生未知错误: {e}")

    def add_one_time_task(self, task_id: str, trigger_time: datetime,
                         callback: Callable, *args, **kwargs):
        """
        添加一次性任务

        Args:
            task_id: 任务ID
            trigger_time: 触发时间（datetime对象，需要是北京时间）
            callback: 回调函数
            *args, **kwargs: 回调函数参数
        """
        try:
            # 确保触发时间是北京时间
            if trigger_time.tzinfo is None:
                trigger_time = self.beijing_tz.localize(trigger_time)
            else:
                trigger_time = trigger_time.astimezone(self.beijing_tz)

            # 检查任务是否已过期
            now = datetime.now(self.beijing_tz)
            if trigger_time <= now:
                logger.warning(f"[ProactiveReply] [定时任务] 任务[{task_id}]触发时间已过期，跳过注册")
                return False

            # 添加任务
            self.scheduler.add_job(
                callback,
                trigger=DateTrigger(run_date=trigger_time, timezone=self.beijing_tz),
                id=task_id,
                args=args,
                kwargs=kwargs,
                replace_existing=True
            )

            logger.info(f"[ProactiveReply] [定时任务] 一次性任务[{task_id}]注册成功，"
                       f"触发时间：{trigger_time.strftime('%Y-%m-%d %H:%M:%S')}（北京时间）")
            return True
        except ValueError as e:
            logger.error(f"[ProactiveReply] [定时任务] 时间格式错误: {e}")
            return False
        except Exception as e:
            logger.error(f"[ProactiveReply] [定时任务] 添加一次性任务失败: {e}")
            return False

    def add_cron_task(self, task_id: str, cron_expr: str, callback: Callable,
                     *args, **kwargs):
        """
        添加定时任务（cron表达式）

        Args:
            task_id: 任务ID
            cron_expr: cron表达式，如 "0 8 * * *" 表示每天8点
            callback: 回调函数
            *args, **kwargs: 回调函数参数
        """
        try:
            # 解析cron表达式
            parts = cron_expr.split()
            if len(parts) != 5:
                logger.error(f"[ProactiveReply] [定时任务] cron表达式格式错误: {cron_expr}")
                return False

            minute, hour, day, month, day_of_week = parts

            self.scheduler.add_job(
                callback,
                trigger=CronTrigger(
                    minute=minute,
                    hour=hour,
                    day=day,
                    month=month,
                    day_of_week=day_of_week,
                    timezone=self.beijing_tz
                ),
                id=task_id,
                args=args,
                kwargs=kwargs,
                replace_existing=True
            )

            logger.info(f"[ProactiveReply] [定时任务] 定时任务[{task_id}]注册成功，"
                       f"cron表达式：{cron_expr}（北京时间）")
            return True
        except ValueError as e:
            logger.error(f"[ProactiveReply] [定时任务] cron表达式格式错误: {e}")
            return False
        except Exception as e:
            logger.error(f"[ProactiveReply] [定时任务] 添加定时任务失败: {e}")
            return False

    def add_daily_task(self, task_id: str, time_str: str, callback: Callable,
                      *args, **kwargs):
        """
        添加每日定时任务

        Args:
            task_id: 任务ID
            time_str: 时间字符串，格式：HH:MM
            callback: 回调函数
            *args, **kwargs: 回调函数参数
        """
        try:
            # 解析时间
            hour, minute = map(int, time_str.split(':'))

            self.scheduler.add_job(
                callback,
                trigger=CronTrigger(
                    hour=hour,
                    minute=minute,
                    timezone=self.beijing_tz
                ),
                id=task_id,
                args=args,
                kwargs=kwargs,
                replace_existing=True
            )

            logger.info(f"[ProactiveReply] [定时任务] 每日任务[{task_id}]注册成功，"
                       f"执行时间：每天{time_str}（北京时间）")
            return True
        except ValueError as e:
            logger.error(f"[ProactiveReply] [定时任务] 时间格式错误: {e}")
            return False
        except Exception as e:
            logger.error(f"[ProactiveReply] [定时任务] 添加每日任务失败: {e}")
            return False

    def remove_task(self, task_id: str):
        """
        移除任务

        Args:
            task_id: 任务ID
        """
        try:
            self.scheduler.remove_job(task_id)
            logger.info(f"[ProactiveReply] [定时任务] 任务[{task_id}]已移除")
        except Exception as e:
            logger.debug(f"[ProactiveReply] [定时任务] 移除任务失败（可能不存在）: {e}")

    def get_beijing_time(self) -> datetime:
        """获取当前北京时间"""
        return datetime.now(self.beijing_tz)

    def is_in_disturb_free_time(self, check_time: datetime,
                                start_hour: int, end_hour: int) -> bool:
        """
        检查时间是否在防打扰时段

        Args:
            check_time: 要检查的时间
            start_hour: 防打扰开始小时
            end_hour: 防打扰结束小时

        Returns:
            是否在防打扰时段
        """
        # 确保是北京时间
        if check_time.tzinfo is None:
            check_time = self.beijing_tz.localize(check_time)
        else:
            check_time = check_time.astimezone(self.beijing_tz)

        hour = check_time.hour

        # 处理跨天情况
        if start_hour <= end_hour:
            return start_hour <= hour < end_hour
        else:
            return hour >= start_hour or hour < end_hour

    def adjust_time_avoid_disturb(self, target_time: datetime,
                                  start_hour: int, end_hour: int) -> datetime:
        """
        调整时间以避开防打扰时段

        Args:
            target_time: 目标时间
            start_hour: 防打扰开始小时
            end_hour: 防打扰结束小时

        Returns:
            调整后的时间
        """
        # 确保是北京时间
        if target_time.tzinfo is None:
            target_time = self.beijing_tz.localize(target_time)
        else:
            target_time = target_time.astimezone(self.beijing_tz)

        if not self.is_in_disturb_free_time(target_time, start_hour, end_hour):
            return target_time

        # 如果在防打扰时段，调整到结束时间
        adjusted = target_time.replace(hour=end_hour, minute=0, second=0, microsecond=0)

        # 如果调整后的时间早于原时间（跨天情况），则使用原时间的日期
        if adjusted < target_time:
            adjusted = adjusted + timedelta(days=1)

        logger.info(f"[ProactiveReply] [时间校验] 触发时间落在防打扰时段，"
                   f"已顺延至{adjusted.strftime('%Y-%m-%d %H:%M')}（北京时间）")

        return adjusted

    def check_task_interval(self, new_time: datetime, existing_times: list,
                           min_interval_hours: int) -> Optional[datetime]:
        """
        检查任务间隔是否满足最小间隔要求

        Args:
            new_time: 新任务时间
            existing_times: 已有任务时间列表
            min_interval_hours: 最小间隔（小时）

        Returns:
            调整后的时间，如果不需要调整则返回原时间
        """
        # 确保是北京时间
        if new_time.tzinfo is None:
            new_time = self.beijing_tz.localize(new_time)
        else:
            new_time = new_time.astimezone(self.beijing_tz)

        min_interval = timedelta(hours=min_interval_hours)

        for existing_time in existing_times:
            if existing_time.tzinfo is None:
                existing_time = self.beijing_tz.localize(existing_time)
            else:
                existing_time = existing_time.astimezone(self.beijing_tz)

            # 计算时间差
            time_diff = abs((new_time - existing_time).total_seconds())

            if time_diff < min_interval.total_seconds():
                # 间隔不足，调整时间
                adjusted = existing_time + min_interval
                logger.info(f"[ProactiveReply] [时间校验] 与已有任务间隔不足，"
                           f"已调整至{adjusted.strftime('%Y-%m-%d %H:%M')}（北京时间）")
                return adjusted

        return new_time
