# telegram-dm-collector-bot

一个用于 **收集 Telegram 私信用户资料**，并支持 **上传 session 账号后采集频道帖子中的 @用户名** 的机器人项目。

当前版本：`0.2.3`

## 第一版已完成

- 基于 **python-telegram-bot 20+**
- 管理端全部使用 **内联按钮交互**
- 正文消息统一走：`<tg-emoji ...>` + `parse_mode='HTML'`
- 按钮图标统一走：`api_kwargs['icon_custom_emoji_id']`
- 支持上传 `.session` 文件，或上传包含 `.session + .json` 的 `.zip` 并立即验证账号状态
- 管理员直接发送 `.session/.zip` 也会自动处理，不再依赖先点按钮保留状态
- session 验证阶段如果遇到兼容异常，也会先入库并标记为“异常”，方便后续继续排查
- 账号列表可查看：
  - 可用
  - 检测中
  - 未登录
  - 异常
  - 采集中
- 支持新建采集任务：
  - 多频道
  - 选择最近几天消息
  - 多账号并发
  - 提取帖子正文中的 `@username`
  - 自动去重
  - 生成 txt 结果文件
- 支持查看任务进度、停止任务、导出结果
- 保留原先 DM 收集链路（用户资料 / 私信入库 / 可选转发管理员）

## 技术栈

- `python-telegram-bot>=20,<23`
- `Telethon==1.41.1`
- `SQLite`

## 项目结构

```text
telegram-dm-collector-bot/
├─ app/
│  ├─ bot.py
│  ├─ collector.py
│  ├─ config.py
│  ├─ database.py
│  ├─ emoji.py
│  └─ version.py
├─ data/                # 运行后自动生成/保存数据库、session、导出文件
├─ .env.example
├─ VERSION
├─ main.py
├─ README.md
└─ requirements.txt
```

## 安装

```bash
pip install -r requirements.txt
```

## 配置

复制 `.env.example` 为 `.env`，至少填写：

```env
BOT_TOKEN=你的机器人Token
ADMIN_IDS=你的Telegram数字ID
API_ID=你的Telegram API_ID
API_HASH=你的Telegram API_HASH
```

### 关键环境变量

- `BOT_TOKEN`：机器人 token
- `ADMIN_IDS`：管理员 ID，多个用英文逗号分隔
- `API_ID` / `API_HASH`：Telethon 连接用户 session 必填
- `DATA_DIR`：数据目录
- `DB_PATH`：SQLite 路径
- `SESSION_DIR`：session 文件目录
- `EXPORT_DIR`：导出结果目录
- `MAX_COLLECT_WORKERS`：最大并发 worker 数

### 会员 emoji 相关

支持自定义这些 custom emoji id：

- `EMOJI_WELCOME_ID`
- `EMOJI_INBOX_ID`
- `EMOJI_STATS_ID`
- `EMOJI_EXPORT_ID`
- `EMOJI_SUCCESS_ID`
- `EMOJI_UPLOAD_ID`
- `EMOJI_WAITING_ID`
- `EMOJI_OK_ID`
- `EMOJI_ERROR_ID`
- `EMOJI_TIMEOUT_ID`
- `EMOJI_PROGRESS_ID`
- `EMOJI_IDEA_ID`

## 启动

```bash
python main.py
```

## 第一版交互说明

### 1. 上传账号
- 进入 **账号管理**
- 点击 **上传 session**
- 发送 `.session` 文件，或发送包含 `.session + .json` 的 `.zip`
- 机器人会自动保存/解压并验证账号状态

### 2. 新建采集
- 进入 **采集中心**
- 点击 **新建采集任务**
- 发送频道列表（一行一个）
- 选择天数
- 选择账号
- 选择并发
- 确认启动

### 3. 查看结果
- 在任务详情页点击 **导出结果**
- 或在 **历史结果** 里重新导出

导出的 txt 内容格式：

```text
@username1
@username2
@username3
```

## 当前限制（第一版已知）

- 目前只支持上传已登录的 `.session` 文件，不包含手机号验证码登录流程
- 频道采集依赖上传的账号是否能访问目标频道
- 提取范围目前以消息正文中的 `@用户名` 为主，不含按钮链接深挖
- 任务停止后会保留已采集到的部分结果，可继续导出

## 后续适合继续迭代的方向

- session 二次登录/验证码流程
- 自动重试和失败频道切号重跑
- 关键词过滤和多种导出格式
- 更细的任务统计和分页结果查看
- Web 后台
