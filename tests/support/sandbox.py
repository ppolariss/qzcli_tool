"""沙箱 HOME —— 让测试可以随便糟蹋本地状态，而不碰用户真实的 ``~/.qzcli``。

## 为什么需要它

qzcli 的一整类 bug（新建计算组被误判、``create`` 不带 ``--spec`` 提不了任务、
``avail`` 静默跳过工作空间）根因都是**代码把本地缓存当成事实的全集**。要覆盖这类
bug，测试就必须能构造出各种残缺的缓存态 —— 也就是必须写本地状态。

在此之前只能拿真实的 ``~/.qzcli`` 做实验（删 specs、删计算组、换假 cookie 再还原），
这本身就是隐患：跑挂了就把用户的登录态和缓存留在半坏状态。

## 一个必须知道的坑

``config.py`` 的路径常量是**模块加载时求值**的::

    CONFIG_DIR = Path.home() / ".qzcli"      # config.py:40
    RESOURCES_FILE = CONFIG_DIR / "resources.json"

所以**测试里改 ``HOME`` 环境变量是没用的** —— 那些常量早在 import 时就固化了。

更隐蔽的是：``cli.py`` 和 ``api.py`` 都写了 ``from .config import CONFIG_DIR``，
这是**按值拷贝**。只 patch ``config.CONFIG_DIR`` 不会影响 ``cli.CONFIG_DIR`` 和
``api.CONFIG_DIR``，它们仍然指向真实 HOME。

因此本模块同时做三件事，缺一不可：

1. 改 ``HOME`` 环境变量（覆盖运行期才求值的 ``Path.home()`` 调用）
2. patch ``config`` 里全部 8 个模块级路径常量
3. patch ``cli`` / ``api`` 里那两份 ``CONFIG_DIR`` 拷贝

这些细节在这里封死，不让每个用例自己处理 —— 漏掉任何一条，测试就会悄悄写到真实
配置目录里，而且**不会报错**，只会在某次跑测试之后发现登录态没了。
"""

import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path

from qzcli import api, cli, config

# 真实 HOME，在**任何沙箱生效之前**（本模块 import 时）固化。
# 不能在 config_paths_are_sandboxed() 里现算 —— 那时 $HOME 已经被沙箱改掉了，
# 算出来的"真实 HOME"就是沙箱自己，自检会把每一条都误判成漏网。
_REAL_HOME = Path(os.path.expanduser("~")).resolve()


def real_home():
    """沙箱**之外**的真实 HOME。

    自检类用例（"确认没写到用户真实目录"）必须用它，**不能**在沙箱里现算
    ``Path.home()`` / ``os.path.expanduser("~")`` —— 那时 ``$HOME`` 已经被指向
    沙箱，算出来的"真实 HOME"就是沙箱自己，于是自检要么恒假绿、要么把沙箱文件
    误报成泄漏。

    这个坑已经踩过两次（``config_paths_are_sandboxed`` 一次、``jobs.json``
    隔离自检一次），所以在这里固化成一个函数，别再各自 expanduser。
    """
    return _REAL_HOME


# config.py 里所有由 CONFIG_DIR 派生的模块级常量。新增常量时必须同步加到这里，
# 否则该常量会绕过沙箱指向真实 HOME。相对路径写法便于在沙箱里重建。
_CONFIG_PATH_ATTRS = {
    "CONFIG_FILE": "config.json",
    "JOBS_FILE": "jobs.json",
    "TOKEN_CACHE_FILE": ".token_cache",
    "COOKIE_FILE": ".cookie",
    "DEFAULT_ENV_FILE": ".env",
    "CREATE_INTERACTIVE_SNAPSHOT_FILE": "create_interactive_snapshot.json",
    "RESOURCES_FILE": "resources.json",
}

# 那些把 CONFIG_DIR 按值 import 进去的模块。
_MODULES_WITH_CONFIG_DIR_COPY = (cli, api)

# 会影响 qzcli 行为、必须在沙箱里清掉的环境变量，免得跑测试的人自己的 shell
# 里恰好设了某个值，导致测试结果因机器而异。
_ENV_TO_CLEAR = (
    "QZCLI_ENV_FILE",
    "QZCLI_SESSION_ID",
    "QZCLI_COOKIE",
    "QZCLI_USERNAME",
    "QZCLI_PASSWORD",
    "QZCLI_BASE_URL",
    # 代理变量同样要清。开发机上常年挂着 Clash 之类（ALL_PROXY=socks5h://…:7897），
    # 不清的话 get_proxy() 会读到它，代理相关用例的结果就因人而异 —— 在我机器上
    # 绿、在 CI 上红，或者反过来。真踩到过：一条断言"无代理时 get_proxy 返回空"
    # 的用例，在本机拿到了 shell 里的 Clash 地址。
    "ALL_PROXY",
    "all_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "NO_PROXY",
    "no_proxy",
)


def config_paths_are_sandboxed():
    """自检：当前进程的路径常量是不是都落在沙箱里。

    返回 ``(ok, offenders)``。``offenders`` 列出仍然指向真实 HOME 的常量名，
    用于在自检用例里给出可读的失败信息。
    """
    offenders = []

    def _check(label, value):
        try:
            Path(value).resolve().relative_to(_REAL_HOME)
        except ValueError:
            return  # 不在真实 HOME 下 —— 正常
        offenders.append(f"{label}={value}")

    _check("config.CONFIG_DIR", config.CONFIG_DIR)
    for attr in _CONFIG_PATH_ATTRS:
        _check(f"config.{attr}", getattr(config, attr))
    for mod in _MODULES_WITH_CONFIG_DIR_COPY:
        _check(f"{mod.__name__}.CONFIG_DIR", mod.CONFIG_DIR)
    return (not offenders), offenders


@contextmanager
def sandbox_home(resources=None, cookie=None, config_json=None, jobs=None, env=None):
    """把 qzcli 的全部本地状态重定向到一个临时目录，退出时整个删掉。

    参数都是可选的初始状态：

    - ``resources``  写进 ``resources.json`` 的 dict；``None`` 表示不建该文件
      （对应"全新机器"这一状态，和写入 ``{}`` 是**不同**的场景）
    - ``cookie``     写进 ``.cookie`` 的字符串
    - ``config_json`` 写进 ``config.json`` 的 dict
    - ``jobs``       写进 ``jobs.json`` 的 dict/list
    - ``env``        额外设置的环境变量 dict

    ``resources`` 也接受字符串，此时原样写入 —— 用于构造非法 JSON 这类损坏态。

    yield 出沙箱的 ``~/.qzcli`` 路径（``Path``），方便用例直接读写断言。
    """
    import json

    tmp_home = Path(tempfile.mkdtemp(prefix="qzcli-sandbox-"))
    sandbox_dir = tmp_home / ".qzcli"
    sandbox_dir.mkdir(parents=True, exist_ok=True)

    saved_env = {k: os.environ.get(k) for k in ("HOME", "USERPROFILE") + _ENV_TO_CLEAR}
    if env:
        saved_env.update({k: os.environ.get(k) for k in env})

    saved_config_dir = config.CONFIG_DIR
    saved_attrs = {a: getattr(config, a) for a in _CONFIG_PATH_ATTRS}
    saved_module_dirs = {m: m.CONFIG_DIR for m in _MODULES_WITH_CONFIG_DIR_COPY}

    try:
        os.environ["HOME"] = str(tmp_home)
        os.environ["USERPROFILE"] = str(tmp_home)  # Windows 上 Path.home() 读这个
        for key in _ENV_TO_CLEAR:
            os.environ.pop(key, None)
        for key, value in (env or {}).items():
            os.environ[key] = value

        config.CONFIG_DIR = sandbox_dir
        for attr, name in _CONFIG_PATH_ATTRS.items():
            setattr(config, attr, sandbox_dir / name)
        for mod in _MODULES_WITH_CONFIG_DIR_COPY:
            mod.CONFIG_DIR = sandbox_dir

        if resources is not None:
            text = (
                resources
                if isinstance(resources, str)
                else json.dumps(resources, ensure_ascii=False)
            )
            (sandbox_dir / "resources.json").write_text(text, encoding="utf-8")
        if cookie is not None:
            (sandbox_dir / ".cookie").write_text(cookie, encoding="utf-8")
        if config_json is not None:
            (sandbox_dir / "config.json").write_text(
                json.dumps(config_json, ensure_ascii=False), encoding="utf-8"
            )
        if jobs is not None:
            (sandbox_dir / "jobs.json").write_text(
                json.dumps(jobs, ensure_ascii=False), encoding="utf-8"
            )

        yield sandbox_dir
    finally:
        config.CONFIG_DIR = saved_config_dir
        for attr, value in saved_attrs.items():
            setattr(config, attr, value)
        for mod, value in saved_module_dirs.items():
            mod.CONFIG_DIR = value
        for key, value in saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        shutil.rmtree(tmp_home, ignore_errors=True)
