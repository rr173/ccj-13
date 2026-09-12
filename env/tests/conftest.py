import os
import tempfile

# 在任何 app 模块导入前生效: 独立测试库 + 关闭计划后台线程
# (测试通过 service 函数手动驱动计划 tick, 保证时序确定)
os.environ.setdefault("DATABASE_URL",
                      f"sqlite:///{tempfile.mkdtemp(prefix='migration-test-')}/test.db")
os.environ.setdefault("APP_VERSION", "1.0.0-test")
os.environ["PLAN_WORKER_ENABLED"] = "0"
